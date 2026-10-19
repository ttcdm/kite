//! `kite init` and `kite connect`: a starter team and the MCP snippets that
//! plug an agent host into it.

use anyhow::bail;
use std::path::Path;

pub const KITE_TOML: &str = r#"# Kite team: who exists, what they may say, and to whom.
# Nothing that is not declared here can be sent.

[broker]
mailbox_capacity = 64
# Coding tasks are slow: give requests 30 minutes unless the sender asks otherwise.
default_timeout_ms = 1800000

[schemas."task.assign@1"]
kind = "request"
response = "task.result@1"
description = "One unit of work: a goal and the files it may touch."
schema_file = "schemas/task.assign.v1.json"

[schemas."task.result@1"]
kind = "response"
schema_file = "schemas/task.result.v1.json"

[schemas."review.request@1"]
kind = "request"
response = "review.verdict@1"
description = "Finished work for review."
schema_file = "schemas/review.request.v1.json"

[schemas."review.verdict@1"]
kind = "response"
schema_file = "schemas/review.verdict.v1.json"

[schemas."status.note@1"]
kind = "event"
description = "A progress note; no answer expected."
schema_file = "schemas/status.note.v1.json"

[agents.lead]
description = "Splits the goal into tasks, hands them out and asks for review."

[agents.coder]
description = "Implements one task at a time and reports what changed."

[agents.reviewer]
description = "Reviews finished work and approves it or says what to fix."

# To have `kite up` launch an agent headless, give it a command. {prompt},
# {agent}, {socket} and {root} are filled in. Check the flags against the
# version of the tool you have installed; the line below is an example only.
# command = ["claude", "-p", "{prompt}", "--mcp-config", "{root}/.kite/mcp/coder.json"]
# prompt = "You are coder. Loop: kite_recv, do each task, answer with kite_respond."
# restart = "on-failure"
# max_restarts = 3

# Agent Kanban: who may shape, assign, merge and archive cards, and what a card
# must pass before it merges. See `kite board`.
[board]
managers = ["lead"]
default_gates = ["tests", "diff_review", "risk"]
independent_review = true

[[routes]]
from = "lead"
to = "coder"
schemas = ["task.assign@1"]

[[routes]]
from = "lead"
to = "reviewer"
schemas = ["review.request@1"]

[[routes]]
from = ["coder", "reviewer"]
to = "lead"
schemas = ["status.note@1"]
"#;

const SCHEMAS: &[(&str, &str)] = &[
    (
        "task.assign.v1.json",
        r#"{
  "type": "object",
  "required": ["goal"],
  "additionalProperties": false,
  "properties": {
    "goal": { "type": "string", "minLength": 1 },
    "files": { "type": "array", "items": { "type": "string" } },
    "acceptance": { "type": "array", "items": { "type": "string" } }
  }
}
"#,
    ),
    (
        "task.result.v1.json",
        r#"{
  "type": "object",
  "required": ["summary", "files_changed"],
  "additionalProperties": false,
  "properties": {
    "summary": { "type": "string", "minLength": 1 },
    "files_changed": { "type": "array", "items": { "type": "string" } },
    "tests": { "type": "string", "enum": ["passed", "failed", "not_run"] }
  }
}
"#,
    ),
    (
        "review.request.v1.json",
        r#"{
  "type": "object",
  "required": ["summary", "files"],
  "additionalProperties": false,
  "properties": {
    "summary": { "type": "string" },
    "files": { "type": "array", "items": { "type": "string" }, "minItems": 1 }
  }
}
"#,
    ),
    (
        "review.verdict.v1.json",
        r#"{
  "type": "object",
  "required": ["approved"],
  "additionalProperties": false,
  "properties": {
    "approved": { "type": "boolean" },
    "notes": { "type": "string" }
  }
}
"#,
    ),
    (
        "status.note.v1.json",
        r#"{
  "type": "object",
  "required": ["text"],
  "additionalProperties": false,
  "properties": { "text": { "type": "string", "maxLength": 2000 } }
}
"#,
    ),
];

/// Writes kite.toml and schemas/ into `dir`. Refuses to overwrite unless `force`.
pub fn write(dir: &Path, force: bool) -> anyhow::Result<Vec<String>> {
    let toml = dir.join("kite.toml");
    if toml.exists() && !force {
        bail!("{} already exists (use --force to overwrite)", toml.display());
    }
    let mut written = vec!["kite.toml".to_string()];
    std::fs::write(&toml, KITE_TOML)?;
    std::fs::create_dir_all(dir.join("schemas"))?;
    for (name, body) in SCHEMAS {
        let p = dir.join("schemas").join(name);
        if !p.exists() || force {
            std::fs::write(&p, body)?;
            written.push(format!("schemas/{name}"));
        }
    }
    Ok(written)
}

/// The MCP server entry for one agent, in the shape most hosts share.
pub fn mcp_json(kite: &str, agent: &str, config: &Path) -> serde_json::Value {
    serde_json::json!({
        "mcpServers": {
            "kite": {
                "command": kite,
                "args": ["mcp", "--agent", agent, "--config", config.display().to_string()]
            }
        }
    })
}

/// How to plug `agent` into common hosts. These follow each host's documented
/// MCP configuration format; check them against the version you run.
pub fn connect_text(kite: &str, agent: &str, config: &Path) -> String {
    let c = config.display();
    let json = serde_json::to_string_pretty(&mcp_json(kite, agent, config)).expect("serializes");
    format!(
        "Connect a host as agent \"{agent}\":\n\n\
         Claude Code\n  claude mcp add kite -- {kite} mcp --agent {agent} --config {c}\n\n\
         Hosts with a JSON mcpServers file (Claude Code .mcp.json, Cursor .cursor/mcp.json,\n\
         Gemini CLI settings.json, Windsurf, Cline):\n{json}\n\n\
         Codex CLI (~/.codex/config.toml)\n  [mcp_servers.kite]\n  command = \"{kite}\"\n  args = [\"mcp\", \"--agent\", \"{agent}\", \"--config\", \"{c}\"]\n\n\
         Any agent that can run shell commands\n  {kite} recv --as {agent} --wait 20\n  {kite} respond --as {agent} --request <id> --result '{{...}}'\n\n\
         Claude Code lease guard (blocks edits to files another agent leased), in .claude/settings.json:\n  \
         {{\"hooks\": {{\"PreToolUse\": [{{\"matcher\": \"Edit|Write|MultiEdit|NotebookEdit\",\n  \
         \"hooks\": [{{\"type\": \"command\", \"command\": \"{kite} hook claude-code --agent {agent} --config {c}\"}}]}}]}}}}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_starter_team_loads() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), false).unwrap();
        let c = kite_proto::Config::load(&dir.path().join("kite.toml")).unwrap();
        assert_eq!(c.agents.len(), 3);
        assert!(c.allowed("lead", "coder", &"task.assign@1".parse().unwrap()));
        assert!(write(dir.path(), false).is_err(), "refuses to overwrite");
    }

    #[test]
    fn the_example_plan_fits_the_starter_team() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), false).unwrap();
        let c = kite_proto::Config::load(&dir.path().join("kite.toml")).unwrap();
        let plan = kite_core::runner::Plan::parse(include_str!("../../../examples/plan.toml")).unwrap();
        for t in &plan.tasks {
            assert!(c.allowed(&plan.as_agent, &t.to, &t.schema), "no route for task {}", t.id);
            c.registry.validate(&t.schema, &t.payload).unwrap_or_else(|e| {
                // Payloads with ${...} references are checked after rendering, at run time.
                assert!(t.payload.to_string().contains("${"), "task {}: {e}", t.id);
            });
        }
    }
}
