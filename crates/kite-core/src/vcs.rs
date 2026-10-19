//! What the board asks of version control. The broker runs these calls on a
//! blocking worker thread, never on its own task, so a slow checkout delays only
//! the card waiting for it. The board's unit tests use a fake.
//!
//! Each git command runs in its own process group with a deadline: on expiry
//! the whole group is killed, hooks included.

use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Longest git error kept in a refusal message.
const MAX_GIT_ERROR: usize = 500;

pub trait Vcs: Send + Sync {
    /// Whether the project root is inside a git work tree. When it is not,
    /// the board checks nothing in git.
    fn is_repo(&self) -> bool;
    /// Makes `dir` a worktree with `branch` checked out: adds the worktree
    /// (creating the branch from HEAD if it does not exist), or checks the
    /// branch out in a worktree that is already there.
    fn checkout_worktree(&self, dir: &Path, branch: &str) -> Result<(), String>;
    /// Creates `branch` at HEAD unless it already exists.
    fn ensure_branch(&self, branch: &str) -> Result<(), String>;
    /// The full id of the commit `rev` names.
    fn resolve_commit(&self, rev: &str) -> Result<String, String>;
    /// Whether `commit` is `rev` or one of its ancestors.
    fn reachable_from(&self, commit: &str, rev: &str) -> Result<bool, String>;
}

/// The `git` command line, run in the project root.
pub struct Git {
    root: PathBuf,
    timeout: Duration,
}

enum Fail {
    /// `git` could not be started.
    Spawn(String),
    /// `git` ran and exited with this code and message.
    Exit(Option<i32>, String),
}

impl Fail {
    fn message(self) -> String {
        match self {
            Fail::Spawn(m) | Fail::Exit(_, m) => m,
        }
    }
}

fn clip(s: &str) -> String {
    let s = s.trim();
    match s.char_indices().nth(MAX_GIT_ERROR) {
        Some((i, _)) => format!("{}...", &s[..i]),
        None => s.to_string(),
    }
}

/// Refuses names git would read as an option before git sees them.
fn not_an_option(what: &str, s: &str) -> Result<(), String> {
    if s.is_empty() || s.starts_with('-') {
        return Err(format!("{what} {s:?} must not be empty or start with -"));
    }
    Ok(())
}

fn drain(mut pipe: impl Read + Send + 'static) -> JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = pipe.read_to_end(&mut buf);
        buf
    })
}

/// Kills every process in the group led by `pid`.
fn kill_group(pid: u32) {
    if let Ok(pid) = i32::try_from(pid)
        && pid > 1
    {
        // SAFETY: kill(2) takes plain integers; a negative pid addresses the process group.
        unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
}

impl Git {
    /// `timeout` bounds each git command, including any hooks it runs.
    pub fn new(root: PathBuf, timeout: Duration) -> Git {
        Git { root, timeout }
    }

    fn run(&self, dir: &Path, args: &[&str]) -> Result<String, Fail> {
        let mut child = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            // Kite may itself run under a git hook; the repository is the project's, not the caller's.
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_OBJECT_DIRECTORY")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .map_err(|e| Fail::Spawn(format!("cannot run git: {e}")))?;
        let pid = child.id();
        let out = drain(child.stdout.take().expect("stdout is piped"));
        let err = drain(child.stderr.take().expect("stderr is piped"));
        let deadline = Instant::now() + self.timeout;
        let timed_out = || {
            let what = args.join(" ");
            Fail::Exit(
                None,
                format!(
                    "git {what} timed out after {} ms; if a worktree was left half made, run `git worktree prune`",
                    self.timeout.as_millis()
                ),
            )
        };
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() >= deadline => {
                    kill_group(pid);
                    let _ = child.wait();
                    let _ = (out.join(), err.join());
                    return Err(timed_out());
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(5)),
                Err(e) => {
                    kill_group(pid);
                    return Err(Fail::Spawn(format!("cannot wait for git: {e}")));
                }
            }
        };
        // git is done, but a hook it ran may still hold the pipes open: give the
        // readers until the deadline, then end the group so they can finish.
        while !(out.is_finished() && err.is_finished()) {
            if Instant::now() >= deadline {
                kill_group(pid);
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let stdout = out.join().unwrap_or_default();
        let stderr = err.join().unwrap_or_default();
        if status.success() {
            return Ok(String::from_utf8_lossy(&stdout).trim().to_string());
        }
        let stderr = String::from_utf8_lossy(&stderr);
        let why = if stderr.trim().is_empty() { "failed".to_string() } else { clip(&stderr) };
        Err(Fail::Exit(status.code(), format!("git {}: {why}", args.join(" "))))
    }

    fn git(&self, args: &[&str]) -> Result<String, String> {
        self.run(&self.root, args).map_err(Fail::message)
    }

    fn branch_exists(&self, branch: &str) -> bool {
        self.git(&["rev-parse", "--verify", "--quiet", &format!("refs/heads/{branch}")]).is_ok()
    }

    fn check_branch_name(&self, branch: &str) -> Result<(), String> {
        not_an_option("branch", branch)?;
        self.git(&["check-ref-format", "--branch", branch]).map(drop)
    }

    /// Whether `dir` is the top of a worktree of this repository.
    fn is_own_worktree(&self, dir: &Path) -> bool {
        let common = |d: &Path| self.run(d, &["rev-parse", "--path-format=absolute", "--git-common-dir"]).ok();
        let same = |a: &Path, b: &Path| a.canonicalize().ok().is_some_and(|a| b.canonicalize().ok() == Some(a));
        let Ok(top) = self.run(dir, &["rev-parse", "--show-toplevel"]) else { return false };
        let theirs = common(dir);
        same(Path::new(&top), dir) && theirs.is_some() && theirs == common(&self.root)
    }
}

impl Vcs for Git {
    fn is_repo(&self) -> bool {
        match self.run(&self.root, &["rev-parse", "--is-inside-work-tree"]) {
            Ok(out) => out == "true",
            // Without a git binary a repository cannot be read, but it is still one:
            // say so, so that the git steps fail loudly instead of being skipped.
            Err(Fail::Spawn(_)) => self.root.ancestors().any(|a| a.join(".git").exists()),
            Err(Fail::Exit(..)) => false,
        }
    }

    fn checkout_worktree(&self, dir: &Path, branch: &str) -> Result<(), String> {
        self.check_branch_name(branch)?;
        if dir.exists() && self.is_own_worktree(dir) {
            let current = self.run(dir, &["symbolic-ref", "--quiet", "--short", "HEAD"]).ok();
            if current.as_deref() == Some(branch) {
                return Ok(());
            }
            let args: &[&str] =
                if self.branch_exists(branch) { &["switch", branch] } else { &["switch", "-c", branch] };
            return self.run(dir, args).map(drop).map_err(Fail::message);
        }
        let path = dir.to_string_lossy();
        if self.branch_exists(branch) {
            self.git(&["worktree", "add", &path, branch]).map(drop)
        } else {
            self.git(&["worktree", "add", "-b", branch, &path]).map(drop)
        }
    }

    fn ensure_branch(&self, branch: &str) -> Result<(), String> {
        self.check_branch_name(branch)?;
        if self.branch_exists(branch) {
            return Ok(());
        }
        self.git(&["branch", branch]).map(drop)
    }

    fn resolve_commit(&self, rev: &str) -> Result<String, String> {
        not_an_option("ref", rev)?;
        self.git(&["rev-parse", "--verify", "--quiet", "--end-of-options", &format!("{rev}^{{commit}}")])
            .map_err(|_| format!("{rev} does not name a commit"))
    }

    fn reachable_from(&self, commit: &str, rev: &str) -> Result<bool, String> {
        let tip = self.resolve_commit(rev)?;
        match self.run(&self.root, &["merge-base", "--is-ancestor", commit, &tip]) {
            Ok(_) => Ok(true),
            Err(Fail::Exit(Some(1), _)) => Ok(false),
            Err(f) => Err(f.message()),
        }
    }
}
