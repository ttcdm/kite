//! `kite hook claude-code`: a Claude Code PreToolUse hook that blocks a file
//! edit when another agent holds the file's lease. Claude Code treats exit
//! code 2 as "block this tool call" and shows stderr to the model.
//!
//! Without `--strict` an unleased file may be edited; with it, the agent must
//! hold the lease itself. If the broker cannot be reached the edit is allowed
//! and a warning is printed, so a stopped broker never wedges a session.

use crate::paths::{Paths, resource_key};
use kite_core::Client;
use serde_json::Value;
use std::io::Read;

const EDIT_TOOLS: &[&str] = &["Edit", "Write", "MultiEdit", "NotebookEdit"];

pub enum Verdict {
    Allow,
    Block(String),
}

/// Which file a PreToolUse event would change, if any.
pub fn target(event: &Value) -> Option<String> {
    let tool = event.get("tool_name")?.as_str()?;
    if !EDIT_TOOLS.contains(&tool) {
        return None;
    }
    let input = event.get("tool_input")?;
    input.get("file_path").or_else(|| input.get("notebook_path")).and_then(Value::as_str).map(str::to_string)
}

pub async fn decide(paths: &Paths, agent: Option<&str>, strict: bool, event: &Value) -> Verdict {
    let Some(file) = target(event) else { return Verdict::Allow };
    let key = resource_key(&paths.root, &file);
    let client = match Client::connect(&paths.socket).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("kite hook: broker unreachable, allowing edit of {key}: {e}");
            return Verdict::Allow;
        }
    };
    let lease = match client.lease_check(&key).await {
        Ok(r) => r.lease,
        Err(e) => {
            eprintln!("kite hook: lease check failed, allowing edit of {key}: {e}");
            return Verdict::Allow;
        }
    };
    match (lease, agent) {
        (Some(l), Some(me)) if l.holder == me => Verdict::Allow,
        (Some(l), _) => {
            Verdict::Block(format!("kite: {key} is leased by {}; ask them (or wait) before editing it", l.holder))
        }
        (None, Some(_)) if strict => {
            Verdict::Block(format!("kite: take the lease first: call kite_lease with resource \"{key}\""))
        }
        (None, _) => Verdict::Allow,
    }
}

/// Reads the hook event from stdin and returns the process exit code.
pub async fn run(paths: &Paths, agent: Option<&str>, strict: bool) -> i32 {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        return 0;
    }
    let Ok(event) = serde_json::from_str::<Value>(&input) else { return 0 };
    match decide(paths, agent, strict, &event).await {
        Verdict::Allow => 0,
        Verdict::Block(why) => {
            eprintln!("{why}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_edit_tools_have_targets() {
        let edit = json!({"tool_name": "Edit", "tool_input": {"file_path": "/p/a.rs"}});
        assert_eq!(target(&edit).as_deref(), Some("/p/a.rs"));
        let nb = json!({"tool_name": "NotebookEdit", "tool_input": {"notebook_path": "n.ipynb"}});
        assert_eq!(target(&nb).as_deref(), Some("n.ipynb"));
        assert_eq!(target(&json!({"tool_name": "Read", "tool_input": {"file_path": "a"}})), None);
    }
}
