//! Launches the agents that declare a `command` and keeps them running per
//! their restart policy. One task per agent (one_for_one): a crash restarts
//! only that agent. The broker learns each process state, so `kite status`
//! shows it next to the agent's mailbox.
//!
//! Each agent runs as the leader of its own process group, so stopping it
//! reaches whatever it started (an MCP server, a shell, a test runner): SIGTERM
//! to the group, then SIGKILL after `stop_grace_ms`. The group id is kept in
//! `.kite/run/<agent>.pid`; if `kite up` itself is killed with SIGKILL its
//! agents keep running, and the next start warns about them.

use crate::broker::BrokerHandle;
use kite_proto::Config;
use kite_proto::config::{AgentSpec, Restart};
use kite_proto::wire::{ProcessState, ProcessStatus};
use serde_json::json;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub struct Supervisor {
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

/// Replaces `{prompt}`, `{agent}`, `{socket}` and `{root}` in one argument.
pub fn expand(arg: &str, agent: &str, spec: &AgentSpec, socket: &Path, root: &Path) -> String {
    arg.replace("{prompt}", spec.prompt.as_deref().unwrap_or(""))
        .replace("{agent}", agent)
        .replace("{socket}", &socket.display().to_string())
        .replace("{root}", &root.display().to_string())
}

struct Job {
    agent: String,
    spec: AgentSpec,
    socket: PathBuf,
    root: PathBuf,
    log: PathBuf,
    pidfile: PathBuf,
    handle: BrokerHandle,
}

/// Sends `sig` to every process in group `pgid`. Signal 0 only checks the group exists.
fn signal_group(pgid: u32, sig: i32) -> bool {
    let Ok(pgid) = i32::try_from(pgid) else { return false };
    if pgid <= 1 {
        return false;
    }
    // SAFETY: kill(2) takes plain integers and touches no memory; a negative pid
    // addresses the process group.
    unsafe { libc::kill(-pgid, sig) == 0 }
}

/// Process groups recorded by an earlier run whose processes are still alive.
fn stale_groups(run_dir: &Path, config: &Config) -> Vec<(String, u32)> {
    let mut stale = Vec::new();
    for agent in config.agents.keys() {
        let pidfile = run_dir.join(format!("{agent}.pid"));
        let Some(pgid) = std::fs::read_to_string(&pidfile).ok().and_then(|s| s.trim().parse::<u32>().ok()) else {
            continue;
        };
        if signal_group(pgid, 0) {
            stale.push((agent.clone(), pgid));
        } else {
            let _ = std::fs::remove_file(&pidfile);
        }
    }
    stale
}

/// SIGTERM to the group, SIGKILL after `grace`, then a final SIGKILL for anything
/// the leader left behind.
async fn stop_group(child: &mut tokio::process::Child, pgid: Option<u32>, grace: Duration) {
    if let Some(g) = pgid {
        signal_group(g, libc::SIGTERM);
        if tokio::time::timeout(grace, child.wait()).await.is_err() {
            signal_group(g, libc::SIGKILL);
        }
    }
    let _ = child.kill().await;
    if let Some(g) = pgid {
        signal_group(g, libc::SIGKILL);
    }
}

impl Supervisor {
    /// Starts every agent with a `command`. Process output goes to `<log_dir>/<agent>.log`.
    pub fn start(config: &Config, handle: BrokerHandle, log_dir: &Path) -> io::Result<Supervisor> {
        std::fs::create_dir_all(log_dir)?;
        let run_dir = log_dir.parent().unwrap_or(log_dir).join("run");
        std::fs::create_dir_all(&run_dir)?;
        let stale = stale_groups(&run_dir, config);
        if !stale.is_empty() {
            for (agent, pgid) in &stale {
                tracing::warn!(
                    agent,
                    pgid,
                    "agent processes from an earlier kite up are still running; stop them before relying on this run"
                );
            }
            let handle = handle.clone();
            tokio::spawn(async move {
                for (agent, pgid) in stale {
                    handle.note(json!({ "type": "stale_agent_group", "agent": agent, "pgid": pgid })).await;
                }
            });
        }
        let (stop, rx) = watch::channel(false);
        let mut tasks = Vec::new();
        for (agent, spec) in &config.agents {
            if spec.command.is_none() {
                continue;
            }
            let job = Job {
                agent: agent.clone(),
                spec: spec.clone(),
                socket: config.broker.socket.clone(),
                root: config.root.clone(),
                log: log_dir.join(format!("{agent}.log")),
                pidfile: run_dir.join(format!("{agent}.pid")),
                handle: handle.clone(),
            };
            tasks.push(tokio::spawn(supervise(job, rx.clone())));
        }
        Ok(Supervisor { stop, tasks })
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Stops every agent's process group and waits for the supervising tasks to finish.
    pub async fn stop(self) {
        let _ = self.stop.send(true);
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

async fn report(job: &Job, state: ProcessState, pid: Option<u32>, restarts: u32, last_exit: Option<i32>) {
    job.handle.set_process(&job.agent, ProcessStatus { state, pid, restarts, last_exit }).await;
}

async fn supervise(job: Job, mut stop: watch::Receiver<bool>) {
    let argv = job.spec.command.clone().unwrap_or_default();
    let mut restarts = 0u32;
    let mut last_exit = None;
    loop {
        report(&job, ProcessState::Starting, None, restarts, last_exit).await;
        let spawned = spawn(&job, &argv);
        let mut child = match spawned {
            Ok(c) => c,
            Err(e) => {
                job.handle
                    .note(
                        json!({ "type": "spawn_failed", "agent": job.agent, "command": argv, "error": e.to_string() }),
                    )
                    .await;
                report(&job, ProcessState::GaveUp, None, restarts, None).await;
                return;
            }
        };
        let pid = child.id();
        if let Some(p) = pid {
            let _ = std::fs::write(&job.pidfile, p.to_string());
        }
        report(&job, ProcessState::Running, pid, restarts, last_exit).await;
        let grace = Duration::from_millis(job.spec.stop_grace_ms);
        let status = tokio::select! {
            s = child.wait() => s,
            _ = stop.changed() => {
                stop_group(&mut child, pid, grace).await;
                let _ = std::fs::remove_file(&job.pidfile);
                report(&job, ProcessState::Exited, None, restarts, last_exit).await;
                return;
            }
        };
        // The leader is gone; sweep up anything it left running in its group.
        if let Some(p) = pid {
            signal_group(p, libc::SIGKILL);
        }
        let _ = std::fs::remove_file(&job.pidfile);
        last_exit = status.as_ref().ok().and_then(|s| s.code());
        let failed = !status.as_ref().is_ok_and(|s| s.success());
        let again = match job.spec.restart {
            Restart::Never => false,
            Restart::OnFailure => failed,
            Restart::Always => true,
        };
        if !again {
            report(&job, ProcessState::Exited, None, restarts, last_exit).await;
            return;
        }
        if restarts >= job.spec.max_restarts {
            report(&job, ProcessState::GaveUp, None, restarts, last_exit).await;
            return;
        }
        restarts += 1;
        tracing::warn!(agent = job.agent, restarts, last_exit, "agent exited; restarting");
        report(&job, ProcessState::Restarting, None, restarts, last_exit).await;
        let backoff =
            Duration::from_millis(250u64.saturating_mul(1 << (restarts - 1).min(6))).min(Duration::from_secs(10));
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stop.changed() => {
                report(&job, ProcessState::Exited, None, restarts, last_exit).await;
                return;
            }
        }
    }
}

fn spawn(job: &Job, argv: &[String]) -> io::Result<tokio::process::Child> {
    let args: Vec<String> = argv.iter().map(|a| expand(a, &job.agent, &job.spec, &job.socket, &job.root)).collect();
    let log = OpenOptions::new().create(true).append(true).open(&job.log)?;
    let cwd = job.spec.cwd.as_ref().map(|c| job.root.join(c)).unwrap_or_else(|| job.root.clone());
    Command::new(&args[0])
        .args(&args[1..])
        .process_group(0)
        .current_dir(cwd)
        .env("KITE_AGENT", &job.agent)
        .env("KITE_SOCKET", &job.socket)
        .env("KITE_ROOT", &job.root)
        .envs(&job.spec.env)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .spawn()
}
