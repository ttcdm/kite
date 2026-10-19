//! `kite.toml`: the agents, the contracts they speak and who may talk to whom.
//! Nothing that is not declared here can be sent.

use crate::schema::{ContractKind, Registry, SchemaRef};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Fsync {
    /// fsync after every ledger record.
    Always,
    /// fsync when the ledger queue drains (default).
    #[default]
    Batch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Restart {
    /// Run once.
    #[default]
    Never,
    /// Restart after a non-zero exit, up to `max_restarts`.
    OnFailure,
    /// Restart after any exit, up to `max_restarts`.
    Always,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerSettings {
    #[serde(default = "d_capacity")]
    pub mailbox_capacity: usize,
    #[serde(default = "d_timeout")]
    pub default_timeout_ms: u64,
    #[serde(default = "d_max_timeout")]
    pub max_timeout_ms: u64,
    #[serde(default = "d_redeliveries")]
    pub max_redeliveries: u32,
    #[serde(default = "d_max_lease")]
    pub max_lease_ms: u64,
    #[serde(default)]
    pub fsync: Fsync,
    #[serde(default = "d_socket")]
    pub socket: PathBuf,
    #[serde(default = "d_ledger")]
    pub ledger: PathBuf,
}

fn d_capacity() -> usize {
    64
}
fn d_timeout() -> u64 {
    120_000
}
fn d_max_timeout() -> u64 {
    3_600_000
}
fn d_redeliveries() -> u32 {
    3
}
fn d_max_lease() -> u64 {
    3_600_000
}
fn d_socket() -> PathBuf {
    PathBuf::from(".kite/kite.sock")
}
fn d_ledger() -> PathBuf {
    PathBuf::from(".kite/ledger.jsonl")
}

impl Default for BrokerSettings {
    fn default() -> Self {
        BrokerSettings {
            mailbox_capacity: d_capacity(),
            default_timeout_ms: d_timeout(),
            max_timeout_ms: d_max_timeout(),
            max_redeliveries: d_redeliveries(),
            max_lease_ms: d_max_lease(),
            fsync: Fsync::default(),
            socket: d_socket(),
            ledger: d_ledger(),
        }
    }
}

/// `[board]`: who manages the agent Kanban and what a card must pass to merge.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoardSettings {
    /// Agents that may shape, assign, merge and archive cards. Empty: every agent may.
    #[serde(default)]
    pub managers: Vec<String>,
    /// Gates a new card must pass, in order, unless it names its own.
    #[serde(default = "d_gates")]
    pub default_gates: Vec<String>,
    /// At least one passing gate must be recorded by someone other than the card's owner.
    #[serde(default = "d_true")]
    pub independent_review: bool,
    /// In a git project, a merged card's `merge_ref` must be a commit reachable from this ref.
    #[serde(default = "d_mainline")]
    pub mainline: String,
}

fn d_gates() -> Vec<String> {
    vec!["tests".into(), "diff_review".into(), "risk".into()]
}
fn d_true() -> bool {
    true
}
fn d_mainline() -> String {
    "main".into()
}

impl Default for BoardSettings {
    fn default() -> Self {
        BoardSettings {
            managers: Vec::new(),
            default_gates: d_gates(),
            independent_review: true,
            mainline: d_mainline(),
        }
    }
}

impl BoardSettings {
    pub fn is_manager(&self, agent: &str) -> bool {
        self.managers.is_empty() || self.managers.iter().any(|m| m == agent)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSchema {
    kind: ContractKind,
    #[serde(default)]
    response: Option<SchemaRef>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    schema: Option<toml::Value>,
    #[serde(default)]
    schema_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    #[serde(default)]
    pub description: Option<String>,
    /// If set, `kite up` launches and supervises this process. `{prompt}`,
    /// `{agent}` and `{socket}` in any argument are replaced.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub restart: Restart,
    #[serde(default = "d_restarts")]
    pub max_restarts: u32,
}

fn d_restarts() -> u32 {
    3
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRoute {
    from: OneOrMany,
    to: OneOrMany,
    schemas: Vec<SchemaRef>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    broker: BrokerSettings,
    #[serde(default)]
    schemas: BTreeMap<String, RawSchema>,
    #[serde(default)]
    agents: BTreeMap<String, AgentSpec>,
    #[serde(default)]
    routes: Vec<RawRoute>,
    #[serde(default)]
    board: BoardSettings,
}

/// A validated configuration. Paths are absolute.
#[derive(Debug)]
pub struct Config {
    pub broker: BrokerSettings,
    pub registry: Registry,
    pub agents: BTreeMap<String, AgentSpec>,
    /// `(from, to)` → the request and event schemas `from` may send `to`.
    pub routes: HashMap<(String, String), BTreeSet<SchemaRef>>,
    pub board: BoardSettings,
    /// Directory holding kite.toml; relative paths resolve against it.
    pub root: PathBuf,
}

/// The name the broker uses as `from` on messages it originates.
pub const BROKER_AGENT: &str = "kite";

pub fn valid_agent_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        && name != BROKER_AGENT
}

pub fn valid_gate_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let root = path.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from("."));
        let root = std::fs::canonicalize(if root.as_os_str().is_empty() { Path::new(".") } else { &root })
            .map_err(|e| format!("cannot resolve {}: {e}", root.display()))?;
        Config::parse(&text, &root).map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn parse(text: &str, root: &Path) -> Result<Config, String> {
        let raw: RawConfig = toml::from_str(text).map_err(|e| e.to_string())?;
        let mut broker = raw.broker;
        if broker.mailbox_capacity == 0 {
            return Err("broker.mailbox_capacity must be at least 1".into());
        }
        if broker.default_timeout_ms == 0 || broker.default_timeout_ms > broker.max_timeout_ms {
            return Err("broker.default_timeout_ms must be between 1 and max_timeout_ms".into());
        }
        broker.socket = root.join(&broker.socket);
        broker.ledger = root.join(&broker.ledger);

        let mut registry = Registry::with_builtins();
        for (name, s) in raw.schemas {
            let schema_ref: SchemaRef = name.parse().map_err(|e: crate::KiteError| e.message)?;
            let json_schema: Value = match (s.schema, s.schema_file) {
                (Some(inline), None) => serde_json::to_value(inline).map_err(|e| e.to_string())?,
                (None, Some(file)) => {
                    let p = root.join(file);
                    let t = std::fs::read_to_string(&p)
                        .map_err(|e| format!("{schema_ref}: cannot read {}: {e}", p.display()))?;
                    serde_json::from_str(&t).map_err(|e| format!("{}: {e}", p.display()))?
                }
                _ => return Err(format!("{schema_ref}: set exactly one of schema or schema_file")),
            };
            registry.insert(schema_ref, s.kind, s.response, s.description, json_schema)?;
        }
        registry.check_links()?;

        for name in raw.agents.keys() {
            if !valid_agent_name(name) {
                return Err(format!(
                    "agent name {name:?} must match [a-z][a-z0-9_-]{{0,63}} and not be \"{BROKER_AGENT}\""
                ));
            }
        }
        for (name, a) in &raw.agents {
            if let Some(cmd) = &a.command
                && cmd.is_empty()
            {
                return Err(format!("agents.{name}.command is empty"));
            }
        }

        let mut routes: HashMap<(String, String), BTreeSet<SchemaRef>> = HashMap::new();
        for (i, r) in raw.routes.into_iter().enumerate() {
            let list = |v: OneOrMany| match v {
                OneOrMany::One(s) => vec![s],
                OneOrMany::Many(v) => v,
            };
            let (froms, tos) = (list(r.from), list(r.to));
            for a in froms.iter().chain(tos.iter()) {
                if !raw.agents.contains_key(a) {
                    return Err(format!("routes[{i}]: agent {a:?} is not declared under [agents]"));
                }
            }
            for s in &r.schemas {
                match registry.get(s) {
                    None => return Err(format!("routes[{i}]: schema {s} is not declared")),
                    Some(c) if c.kind == ContractKind::Response => {
                        return Err(format!(
                            "routes[{i}]: {s} is a response; responses travel back on the request's route"
                        ));
                    }
                    Some(_) => {}
                }
            }
            for f in &froms {
                for t in &tos {
                    if f == t {
                        return Err(format!("routes[{i}]: {f} cannot route to itself"));
                    }
                    routes.entry((f.clone(), t.clone())).or_default().extend(r.schemas.iter().cloned());
                }
            }
        }

        for m in &raw.board.managers {
            if !raw.agents.contains_key(m) {
                return Err(format!("board.managers: {m:?} is not declared under [agents]"));
            }
        }
        for g in &raw.board.default_gates {
            if !valid_gate_name(g) {
                return Err(format!("board.default_gates: {g:?} must match [a-z][a-z0-9_-]{{0,63}}"));
            }
        }
        let m = &raw.board.mainline;
        if m.is_empty() || m.len() > 255 || m.starts_with('-') || m.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(format!("board.mainline: {m:?} must be a git ref name, not empty or starting with -"));
        }
        Ok(Config { broker, registry, agents: raw.agents, routes, board: raw.board, root: root.to_path_buf() })
    }

    pub fn allowed(&self, from: &str, to: &str, schema: &SchemaRef) -> bool {
        self.routes.get(&(from.to_string(), to.to_string())).is_some_and(|s| s.contains(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
        [schemas."task.do@1"]
        kind = "request"
        response = "task.done@1"
        schema = { type = "object", required = ["goal"] }
        [schemas."task.done@1"]
        kind = "response"
        schema = { type = "object" }
        [agents.lead]
        [agents.worker]
        [[routes]]
        from = "lead"
        to = ["worker"]
        schemas = ["task.do@1"]
    "#;

    #[test]
    fn loads_routes_and_resolves_paths() {
        let c = Config::parse(BASE, Path::new("/proj")).unwrap();
        let s: SchemaRef = "task.do@1".parse().unwrap();
        assert!(c.allowed("lead", "worker", &s));
        assert!(!c.allowed("worker", "lead", &s));
        assert_eq!(c.broker.socket, Path::new("/proj/.kite/kite.sock"));
    }

    #[test]
    fn rejects_undeclared_agents_and_response_routes() {
        let bad = BASE.replace("to = [\"worker\"]", "to = [\"ghost\"]");
        assert!(Config::parse(&bad, Path::new("/")).unwrap_err().contains("ghost"));
        let bad = BASE.replace("schemas = [\"task.do@1\"]", "schemas = [\"task.done@1\"]");
        assert!(Config::parse(&bad, Path::new("/")).unwrap_err().contains("response"));
    }

    #[test]
    fn mainline_defaults_to_main_and_is_checked() {
        assert_eq!(Config::parse(BASE, Path::new("/")).unwrap().board.mainline, "main");
        let c = Config::parse(&format!("{BASE}\n[board]\nmainline = \"trunk\"\n"), Path::new("/")).unwrap();
        assert_eq!(c.board.mainline, "trunk");
        for bad in ["", "-x", "a b"] {
            let text = format!("{BASE}\n[board]\nmainline = {bad:?}\n");
            assert!(Config::parse(&text, Path::new("/")).unwrap_err().contains("mainline"), "{bad}");
        }
    }

    #[test]
    fn rejects_unknown_keys() {
        let bad = format!("{BASE}\n[broker]\nmailbox_capacty = 3\n");
        assert!(Config::parse(&bad, Path::new("/")).is_err());
    }

    #[test]
    fn agent_names_are_checked() {
        assert!(valid_agent_name("coder-2"));
        for bad in ["", "Coder", "2coder", "kite", "a b"] {
            assert!(!valid_agent_name(bad), "{bad}");
        }
    }
}
