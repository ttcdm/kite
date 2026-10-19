//! Fixtures for end-to-end tests of the compiled `kite` binary.
//!
//! `Project` is one throwaway Kite project: `kite init`, `kite up` with its
//! stderr kept in `.kite/up.log`, the CLI, and MCP sessions. If a test panics
//! while a `Project` is alive, its evidence (up.log, the ledger, `kite trace`,
//! `kite board`, `kite status`, kite.toml and agent logs) is copied to
//! `target/tmp/e2e-artifacts/<test name>/` before the temp directory goes.
//!
//! Every wait has a limit, so a hung broker or agent fails the test with a
//! message instead of hanging the suite. Waits poll for a state, never sleep
//! for a fixed time and assume.

#![allow(dead_code)]

use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Output, Stdio};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

pub const KITE: &str = env!("CARGO_BIN_EXE_kite");
/// Longest any single step may take before the test fails.
pub const STEP: Duration = Duration::from_secs(30);

pub struct Project {
    name: &'static str,
    _tmp: TempDir,
    pub dir: PathBuf,
    up: Option<Child>,
}

impl Project {
    /// A fresh directory with the starter team from `kite init`.
    pub fn init(name: &'static str) -> Project {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().canonicalize().unwrap();
        let p = Project { name, _tmp: tmp, dir, up: None };
        p.ok(&["init"]);
        p
    }

    pub fn socket(&self) -> PathBuf {
        self.dir.join(".kite/kite.sock")
    }

    pub fn path(&self, rel: &str) -> PathBuf {
        self.dir.join(rel)
    }

    /// Rewrites kite.toml (before `up`).
    pub fn edit_config(&self, f: impl FnOnce(String) -> String) {
        let p = self.path("kite.toml");
        let text = std::fs::read_to_string(&p).unwrap();
        std::fs::write(&p, f(text)).unwrap();
    }

    pub fn write(&self, rel: &str, body: &str) {
        let p = self.path(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    /// The file holding `agent`'s token.
    pub fn token_file(&self, agent: &str) -> PathBuf {
        self.path(&format!(".kite/tokens/{agent}"))
    }

    /// Runs `kite` in the project. A command acting `--as` an agent gets that
    /// agent's `--token-file`, as a configured host would.
    pub fn kite(&self, args: &[&str]) -> Output {
        let mut all: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        if let Some(i) = args.iter().position(|a| *a == "--as")
            && let Some(agent) = args.get(i + 1)
        {
            all.push("--token-file".into());
            all.push(self.token_file(agent).display().to_string());
        }
        self.kite_bare(&all.iter().map(String::as_str).collect::<Vec<_>>())
    }

    /// Runs `kite` exactly as given, with no token added and none inherited.
    pub fn kite_bare(&self, args: &[&str]) -> Output {
        StdCommand::new(KITE)
            .args(args)
            .current_dir(&self.dir)
            .env_remove("KITE_AGENT")
            .env_remove("KITE_TOKEN")
            .env_remove("KITE_TOKEN_FILE")
            .output()
            .unwrap()
    }

    /// Runs `kite` and fails the test, with its stderr, unless it exits 0. Returns stdout.
    pub fn ok(&self, args: &[&str]) -> String {
        let out = self.kite(args);
        assert!(
            out.status.success(),
            "`kite {}` exited {:?}\nstdout: {}\nstderr: {}",
            args.join(" "),
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Runs `kite --json …` and parses stdout.
    pub fn json(&self, args: &[&str]) -> Value {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        let out = self.kite(&all);
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
            panic!(
                "`kite {}` did not print JSON ({e})\nstdout: {}\nstderr: {}",
                all.join(" "),
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )
        })
    }

    /// Runs a command other than kite in the project (git, python3, …) and requires success.
    pub fn sh(&self, program: &str, args: &[&str]) -> String {
        let out = StdCommand::new(program).args(args).current_dir(&self.dir).output().unwrap();
        assert!(
            out.status.success(),
            "`{program} {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Starts `kite up` and waits for its socket. stderr is appended to `.kite/up.log`.
    pub async fn up(&mut self) {
        assert!(self.up.is_none(), "kite up is already running");
        std::fs::create_dir_all(self.path(".kite")).unwrap();
        let log = std::fs::OpenOptions::new().create(true).append(true).open(self.path(".kite/up.log")).unwrap();
        let child = Command::new(KITE)
            .arg("up")
            .current_dir(&self.dir)
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        self.up = Some(child);
        let socket = self.socket();
        self.wait_until("kite up to create its socket", || socket.exists()).await;
    }

    /// Sends SIGTERM to `kite up`, requires a clean exit, and returns the whole up.log.
    pub async fn stop(&mut self) -> String {
        let mut child = self.up.take().expect("kite up is not running");
        let pid = child.id().unwrap().to_string();
        assert!(StdCommand::new("kill").args(["-TERM", &pid]).status().unwrap().success());
        let status = tokio::time::timeout(STEP, child.wait()).await.expect("kite up did not stop on SIGTERM").unwrap();
        let log = self.up_log();
        assert!(status.success(), "kite up exited with {status}\n{log}");
        assert!(log.trim_end().ends_with("kite up: stopped"), "kite up did not report a clean stop:\n{log}");
        assert!(!log.contains("panicked"), "kite up panicked:\n{log}");
        assert!(!self.socket().exists(), "the socket was left behind");
        log
    }

    /// Kills `kite up` with SIGKILL, as a crash would: no cleanup runs.
    pub async fn kill_hard(&mut self) {
        let mut child = self.up.take().expect("kite up is not running");
        child.start_kill().unwrap();
        tokio::time::timeout(STEP, child.wait()).await.expect("kite up did not die on SIGKILL").unwrap();
    }

    pub fn up_log(&self) -> String {
        std::fs::read_to_string(self.path(".kite/up.log")).unwrap_or_default()
    }

    /// Polls `cond` until it holds or STEP passes.
    pub async fn wait_until(&self, what: &str, mut cond: impl FnMut() -> bool) {
        let end = tokio::time::Instant::now() + STEP;
        while !cond() {
            assert!(tokio::time::Instant::now() < end, "timed out waiting for {what}\nup.log:\n{}", self.up_log());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// An MCP session for `agent` with its token, already initialized.
    pub async fn mcp(&self, agent: &str) -> Mcp {
        self.mcp_with(agent, &["--token-file", &self.token_file(agent).display().to_string()]).await
    }

    /// An MCP session for `agent` started with exactly `extra` arguments, already initialized.
    pub async fn mcp_with(&self, agent: &str, extra: &[&str]) -> Mcp {
        let mut m = Mcp::start(&self.dir, agent, extra);
        let init = m
            .rpc(
                "initialize",
                json!({ "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "e2e", "version": "0" } }),
            )
            .await;
        assert_eq!(init["result"]["serverInfo"]["name"], "kite", "{init}");
        m.notify("notifications/initialized").await;
        m
    }

    /// Runs the Claude Code lease hook for an Edit of `file` as `agent`; returns its exit code.
    pub fn hook(&self, agent: &str, file: &Path) -> i32 {
        let mut child = StdCommand::new(KITE)
            .args(["hook", "claude-code", "--agent", agent])
            .current_dir(&self.dir)
            .stdin(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let event = json!({ "tool_name": "Edit", "tool_input": { "file_path": file } });
        child.stdin.take().unwrap().write_all(event.to_string().as_bytes()).unwrap();
        child.wait().unwrap().code().unwrap()
    }

    /// Makes the project a git repository with one commit on `main`.
    pub fn git_init(&self) {
        self.sh("git", &["init", "-q", "-b", "main"]);
        self.sh("git", &["config", "user.name", "e2e"]);
        self.sh("git", &["config", "user.email", "e2e@example.invalid"]);
        self.write(".gitignore", ".kite/\n");
        self.sh("git", &["add", "-A"]);
        self.sh("git", &["commit", "-q", "-m", "init"]);
    }

    fn save_artifacts(&mut self) {
        let out = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("e2e-artifacts").join(self.name);
        let _ = std::fs::remove_dir_all(&out);
        if std::fs::create_dir_all(&out).is_err() {
            return;
        }
        if self.up.is_some() {
            // Live state first, while the broker is still up.
            let _ = std::fs::write(out.join("status.json"), self.kite(&["--json", "status"]).stdout);
            let _ = std::fs::write(out.join("board.txt"), self.kite(&["board", "--all"]).stdout);
        }
        for (from, to) in [(".kite/up.log", "up.log"), ("kite.toml", "kite.toml")] {
            let _ = std::fs::copy(self.path(from), out.join(to));
        }
        if let Ok(segments) = std::fs::read_dir(self.path(".kite/ledger")) {
            for s in segments.flatten() {
                let _ = std::fs::copy(s.path(), out.join(format!("ledger-{}", s.file_name().to_string_lossy())));
            }
        }
        let _ = std::fs::write(out.join("trace.txt"), self.kite(&["trace"]).stdout);
        if let Ok(logs) = std::fs::read_dir(self.path(".kite/logs")) {
            for l in logs.flatten() {
                let _ = std::fs::copy(l.path(), out.join(l.file_name()));
            }
        }
        eprintln!("e2e artifacts for {} saved to {}", self.name, out.display());
    }
}

impl Drop for Project {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.save_artifacts();
        }
        if let Some(mut child) = self.up.take() {
            let _ = child.start_kill();
        }
    }
}

/// One `kite mcp` process, driven over stdio like an agent host would.
pub struct Mcp {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Lines<BufReader<ChildStdout>>,
    next: u64,
}

impl Mcp {
    fn start(dir: &Path, agent: &str, extra: &[&str]) -> Mcp {
        let mut child = Command::new(KITE)
            .args(["mcp", "--agent", agent])
            .args(extra)
            .current_dir(dir)
            .env_remove("KITE_TOKEN")
            .env_remove("KITE_TOKEN_FILE")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        Mcp { child, stdin, lines, next: 1 }
    }

    async fn send(&mut self, v: Value) {
        let line = v.to_string() + "\n";
        self.stdin.as_mut().unwrap().write_all(line.as_bytes()).await.unwrap();
    }

    pub async fn notify(&mut self, method: &str) {
        self.send(json!({ "jsonrpc": "2.0", "method": method })).await;
    }

    pub async fn rpc(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })).await;
        loop {
            let line = tokio::time::timeout(STEP, self.lines.next_line())
                .await
                .unwrap_or_else(|_| panic!("no reply to {method} within {STEP:?}"))
                .unwrap()
                .unwrap_or_else(|| panic!("kite mcp closed stdout while waiting for {method}"));
            let reply: Value = serde_json::from_str(&line).unwrap();
            if reply["id"] == id {
                return reply;
            }
        }
    }

    /// Calls a tool; returns (is_error, structured content).
    pub async fn tool(&mut self, name: &str, args: Value) -> (bool, Value) {
        let r = self.rpc("tools/call", json!({ "name": name, "arguments": args })).await;
        let result = &r["result"];
        let err = result["isError"].as_bool().unwrap_or_else(|| panic!("{name}: no isError in {r}"));
        (err, result["structuredContent"].clone())
    }

    /// Calls a tool that must succeed.
    pub async fn ok(&mut self, name: &str, args: Value) -> Value {
        let (err, v) = self.tool(name, args.clone()).await;
        assert!(!err, "{name}({args}) failed: {v}");
        v
    }

    /// Calls a tool that must fail with `code`; returns the error object.
    pub async fn refused(&mut self, name: &str, args: Value, code: &str) -> Value {
        let (err, v) = self.tool(name, args.clone()).await;
        assert!(err, "{name}({args}) should have failed with {code}, got {v}");
        assert_eq!(v["error"]["code"], code, "{name}({args})");
        v["error"].clone()
    }

    /// Closes stdin and requires the server to exit cleanly.
    pub async fn close(mut self) {
        self.stdin.take();
        let status = tokio::time::timeout(STEP, self.child.wait()).await.expect("kite mcp did not exit").unwrap();
        assert!(status.success(), "kite mcp exited with {status}");
    }
}
