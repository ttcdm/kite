//! Runs a task graph as an ordinary agent. Each task is one blocking request;
//! tasks whose dependencies are done run concurrently up to `max_parallel`.
//! The runner gets no special powers: its requests pass the same routes,
//! schemas, deadlines and deadlock checks as any other agent's.

use crate::client::Client;
use kite_proto::wire::AwaitOk;
use kite_proto::{Draft, ErrorCode, Kind, KiteError, SchemaRef};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use ulid::Ulid;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    /// The declared agent the runner registers as.
    #[serde(rename = "as")]
    pub as_agent: String,
    #[serde(default = "d_parallel")]
    pub max_parallel: usize,
    /// Cancel everything still running when one task fails.
    #[serde(default)]
    pub fail_fast: bool,
    pub tasks: Vec<TaskSpec>,
}

fn d_parallel() -> usize {
    4
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub id: String,
    pub to: String,
    pub schema: SchemaRef,
    /// Strings of the form `${tasks.<id>.result}` or `${tasks.<id>.result.a.b}`
    /// are replaced by that task's result (or part of it). Only tasks listed
    /// in `after` may be referenced.
    pub payload: Value,
    #[serde(default)]
    pub after: Vec<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Done,
    Failed,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskReport {
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Ulid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    pub ok: bool,
    pub tasks: BTreeMap<String, TaskReport>,
}

/// One `${tasks.<id>.result[.path]}` reference.
#[derive(Debug, Clone, PartialEq)]
struct Ref {
    task: String,
    path: Vec<String>,
}

fn parse_ref(inner: &str) -> Option<Ref> {
    let mut parts = inner.split('.');
    if parts.next()? != "tasks" {
        return None;
    }
    let task = parts.next()?.to_string();
    if parts.next()? != "result" {
        return None;
    }
    Some(Ref { task, path: parts.map(str::to_string).collect() })
}

/// Splits a string into literal text and references.
fn segments(s: &str) -> Result<Vec<Result<String, Ref>>, String> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push(Ok(rest[..start].to_string()));
        let after = &rest[start + 2..];
        let end = after.find('}').ok_or_else(|| format!("unclosed ${{ in {s:?}"))?;
        let r = parse_ref(&after[..end]).ok_or_else(|| format!("bad reference ${{{}}}", &after[..end]))?;
        out.push(Err(r));
        rest = &after[end + 1..];
    }
    out.push(Ok(rest.to_string()));
    out.retain(|p| !matches!(p, Ok(t) if t.is_empty()));
    Ok(out)
}

fn refs_in(v: &Value, out: &mut Vec<Ref>) -> Result<(), String> {
    match v {
        Value::String(s) => {
            for seg in segments(s)? {
                if let Err(r) = seg {
                    out.push(r);
                }
            }
        }
        Value::Array(a) => a.iter().try_for_each(|x| refs_in(x, out))?,
        Value::Object(m) => m.values().try_for_each(|x| refs_in(x, out))?,
        _ => {}
    }
    Ok(())
}

fn lookup(results: &HashMap<String, Value>, r: &Ref) -> Result<Value, String> {
    let mut v = results.get(&r.task).ok_or_else(|| format!("no result for task {}", r.task))?;
    for p in &r.path {
        v = match v {
            Value::Object(m) => m.get(p),
            Value::Array(a) => p.parse::<usize>().ok().and_then(|i| a.get(i)),
            _ => None,
        }
        .ok_or_else(|| format!("task {} result has no {}", r.task, r.path.join(".")))?;
    }
    Ok(v.clone())
}

/// Fills references. A string that is exactly one reference becomes the
/// referenced value; references inside longer strings are written as text.
pub fn render(v: &Value, results: &HashMap<String, Value>) -> Result<Value, String> {
    Ok(match v {
        Value::String(s) => {
            let segs = segments(s)?;
            if let [Err(r)] = segs.as_slice() {
                lookup(results, r)?
            } else {
                let mut text = String::new();
                for seg in segs {
                    match seg {
                        Ok(t) => text.push_str(&t),
                        Err(r) => match lookup(results, &r)? {
                            Value::String(s) => text.push_str(&s),
                            other => text.push_str(&other.to_string()),
                        },
                    }
                }
                Value::String(text)
            }
        }
        Value::Array(a) => Value::Array(a.iter().map(|x| render(x, results)).collect::<Result<_, _>>()?),
        Value::Object(m) => {
            Value::Object(m.iter().map(|(k, x)| Ok((k.clone(), render(x, results)?))).collect::<Result<_, String>>()?)
        }
        other => other.clone(),
    })
}

impl Plan {
    pub fn parse(text: &str) -> Result<Plan, String> {
        let plan: Plan = toml::from_str(text).map_err(|e| e.to_string())?;
        plan.check()?;
        Ok(plan)
    }

    /// Unique ids, known dependencies, no cycles, references only to dependencies.
    pub fn check(&self) -> Result<(), String> {
        if self.max_parallel == 0 {
            return Err("max_parallel must be at least 1".into());
        }
        let ids: BTreeSet<&str> = self.tasks.iter().map(|t| t.id.as_str()).collect();
        if ids.len() != self.tasks.len() {
            return Err("task ids must be unique".into());
        }
        for t in &self.tasks {
            for d in &t.after {
                if !ids.contains(d.as_str()) {
                    return Err(format!("task {}: after names unknown task {d}", t.id));
                }
            }
            let mut refs = Vec::new();
            refs_in(&t.payload, &mut refs).map_err(|e| format!("task {}: {e}", t.id))?;
            for r in refs {
                if !t.after.contains(&r.task) {
                    return Err(format!("task {}: uses {}'s result but does not list it in after", t.id, r.task));
                }
            }
        }
        self.order().map(|_| ())
    }

    /// A topological order (Kahn's algorithm), or the tasks stuck in a cycle.
    pub fn order(&self) -> Result<Vec<String>, String> {
        let mut indegree: BTreeMap<&str, usize> = self.tasks.iter().map(|t| (t.id.as_str(), t.after.len())).collect();
        let mut queue: VecDeque<&str> = indegree.iter().filter(|(_, n)| **n == 0).map(|(id, _)| *id).collect();
        let mut out = Vec::new();
        while let Some(id) = queue.pop_front() {
            out.push(id.to_string());
            for t in self.tasks.iter().filter(|t| t.after.iter().any(|d| d == id)) {
                let n = indegree.get_mut(t.id.as_str()).expect("every task has an indegree");
                *n -= 1;
                if *n == 0 {
                    queue.push_back(&t.id);
                }
            }
        }
        if out.len() != self.tasks.len() {
            let stuck: Vec<&str> = indegree.iter().filter(|(_, n)| **n > 0).map(|(id, _)| *id).collect();
            return Err(format!("tasks form a cycle: {}", stuck.join(", ")));
        }
        Ok(out)
    }
}

enum Update {
    Sent(String, Ulid),
    Finished(String, TaskReport),
}

async fn run_task(client: Arc<Client>, task: TaskSpec, payload: Value, updates: mpsc::Sender<Update>) {
    let fail = |e: KiteError| TaskReport {
        state: if e.code == ErrorCode::Cancelled { TaskState::Cancelled } else { TaskState::Failed },
        request_id: None,
        result: None,
        error: Some(serde_json::to_value(&e).expect("errors serialize")),
    };
    let draft = Draft {
        id: None,
        to: task.to.clone(),
        schema: task.schema.clone(),
        kind: Some(Kind::Request),
        payload,
        timeout_ms: task.timeout_ms,
        blocking: true,
    };
    let sent = match client.send(draft).await {
        Ok(s) => s,
        Err(e) => {
            let _ = updates.send(Update::Finished(task.id, fail(e))).await;
            return;
        }
    };
    let _ = updates.send(Update::Sent(task.id.clone(), sent.id)).await;
    // The broker answers every request by its deadline, so this loop ends.
    let report = loop {
        match client.await_answer(sent.id, 25_000).await {
            Ok(AwaitOk::Pending { .. }) => continue,
            Ok(AwaitOk::Done { response }) => {
                let done = response.kind == Kind::Response;
                break TaskReport {
                    state: if done { TaskState::Done } else { TaskState::Failed },
                    request_id: Some(sent.id),
                    result: done.then(|| response.payload.clone()),
                    error: (!done).then_some(response.payload),
                };
            }
            Err(e) => break TaskReport { request_id: Some(sent.id), ..fail(e) },
        }
    };
    let _ = updates.send(Update::Finished(task.id, report)).await;
}

/// Runs `plan` with a client already registered as `plan.as_agent`.
pub async fn run(client: Arc<Client>, plan: &Plan) -> Result<RunReport, String> {
    plan.check()?;
    let specs: BTreeMap<&str, &TaskSpec> = plan.tasks.iter().map(|t| (t.id.as_str(), t)).collect();
    let mut reports: BTreeMap<String, TaskReport> = BTreeMap::new();
    let mut results: HashMap<String, Value> = HashMap::new();
    let mut running: HashMap<String, Option<Ulid>> = HashMap::new();
    let mut cancelling = false;
    let (tx, mut rx) = mpsc::channel(64);
    let mut set = JoinSet::new();

    loop {
        // Settle every task whose fate is decided by its dependencies, then start what is ready.
        let mut progressed = true;
        while progressed {
            progressed = false;
            for t in &plan.tasks {
                if reports.contains_key(&t.id) || running.contains_key(&t.id) {
                    continue;
                }
                let deps: Vec<Option<TaskState>> = t.after.iter().map(|d| reports.get(d).map(|r| r.state)).collect();
                let blocked = deps.iter().any(|s| matches!(s, Some(st) if *st != TaskState::Done));
                if blocked || cancelling {
                    let why = if cancelling { "fail_fast stopped the run" } else { "a dependency did not finish" };
                    reports.insert(
                        t.id.clone(),
                        TaskReport {
                            state: TaskState::Skipped,
                            request_id: None,
                            result: None,
                            error: Some(json!({ "message": why })),
                        },
                    );
                    progressed = true;
                    continue;
                }
                if deps.iter().all(|s| *s == Some(TaskState::Done)) && running.len() < plan.max_parallel {
                    let payload = match render(&t.payload, &results) {
                        Ok(p) => p,
                        Err(e) => {
                            reports.insert(
                                t.id.clone(),
                                TaskReport {
                                    state: TaskState::Failed,
                                    request_id: None,
                                    result: None,
                                    error: Some(json!({ "message": e })),
                                },
                            );
                            progressed = true;
                            continue;
                        }
                    };
                    running.insert(t.id.clone(), None);
                    set.spawn(run_task(client.clone(), (*specs[t.id.as_str()]).clone(), payload, tx.clone()));
                    progressed = true;
                }
            }
        }
        if reports.len() == plan.tasks.len() {
            break;
        }
        match rx.recv().await {
            Some(Update::Sent(id, rid)) => {
                if cancelling {
                    let _ = client.cancel(rid).await;
                }
                running.insert(id, Some(rid));
            }
            Some(Update::Finished(id, report)) => {
                running.remove(&id);
                if report.state == TaskState::Done {
                    results.insert(id.clone(), report.result.clone().unwrap_or(Value::Null));
                } else if plan.fail_fast && !cancelling {
                    cancelling = true;
                    for rid in running.values().flatten() {
                        let _ = client.cancel(*rid).await;
                    }
                }
                reports.insert(id, report);
            }
            None => return Err("task channel closed unexpectedly".into()),
        }
    }
    while set.join_next().await.is_some() {}
    let ok = reports.values().all(|r| r.state == TaskState::Done);
    Ok(RunReport { ok, tasks: reports })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_whole_and_embedded_references() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), json!({"files": ["x.rs", "y.rs"], "n": 2}));
        let v = render(
            &json!({"all": "${tasks.a.result}", "first": "${tasks.a.result.files.0}", "msg": "n=${tasks.a.result.n}!"}),
            &results,
        )
        .unwrap();
        assert_eq!(v, json!({"all": {"files": ["x.rs", "y.rs"], "n": 2}, "first": "x.rs", "msg": "n=2!"}));
    }

    fn plan(tasks: &str) -> Result<Plan, String> {
        Plan::parse(&format!("as = \"lead\"\n{tasks}"))
    }

    #[test]
    fn rejects_cycles_unknown_deps_and_undeclared_references() {
        let t = |id: &str, after: &str, payload: &str| {
            format!("[[tasks]]\nid = \"{id}\"\nto = \"w\"\nschema = \"t@1\"\npayload = {payload}\nafter = [{after}]\n")
        };
        let cyc = format!("{}{}", t("a", "\"b\"", "{}"), t("b", "\"a\"", "{}"));
        assert!(plan(&cyc).unwrap_err().contains("cycle"));
        assert!(plan(&t("a", "\"zz\"", "{}")).unwrap_err().contains("zz"));
        let undeclared = format!("{}{}", t("a", "", "{}"), t("b", "", "{ x = \"${tasks.a.result}\" }"));
        assert!(plan(&undeclared).unwrap_err().contains("does not list it"));
        let good = format!("{}{}", t("a", "", "{}"), t("b", "\"a\"", "{ x = \"${tasks.a.result}\" }"));
        assert_eq!(plan(&good).unwrap().order().unwrap(), vec!["a", "b"]);
    }
}
