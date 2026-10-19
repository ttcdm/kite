//! Per-agent tokens: `.kite/tokens/<agent>`, one random 256-bit value per
//! declared agent, readable only by the owner. An agent registers by
//! presenting its token, so passing another agent's name is not enough to
//! act as that agent.
//!
//! This is not a defence against a process of the same user that reads the
//! token files on purpose: anything running as you can. See SECURITY.md.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

fn random_token() -> io::Result<String> {
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(hex::encode(bytes))
}

pub fn path(dir: &Path, agent: &str) -> PathBuf {
    dir.join(agent)
}

fn write_new(path: &Path) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = fs::remove_file(&tmp);
    let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp)?;
    f.write_all(format!("{}\n", random_token()?).as_bytes())?;
    f.sync_all()?;
    fs::rename(&tmp, path)
}

/// Makes sure every agent in `agents` has a token and nobody else does, that
/// `dir` is owner-only, and that nothing under `.kite/` is ever committed.
pub fn ensure(dir: &Path, agents: &BTreeSet<String>) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    if let Some(kite_dir) = dir.parent() {
        let ignore = kite_dir.join(".gitignore");
        if !ignore.exists() {
            fs::write(ignore, "# Kite's runtime files: tokens, socket, ledger, logs.\n*\n")?;
        }
    }
    for agent in agents {
        let p = path(dir, agent);
        if !p.exists() {
            write_new(&p)?;
        }
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !agents.contains(&name) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// The agent's current token.
pub fn read(dir: &Path, agent: &str) -> io::Result<String> {
    Ok(fs::read_to_string(path(dir, agent))?.trim().to_string())
}

/// Replaces the agent's token. Connections already registered keep working
/// until they reconnect; new registrations need the new token.
pub fn rotate(dir: &Path, agent: &str) -> io::Result<()> {
    write_new(&path(dir, agent))
}

/// Compares in time that depends only on the lengths, not on where they differ.
pub fn matches(given: &str, expected: &str) -> bool {
    let (a, b) = (given.as_bytes(), expected.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= (*a.get(i).unwrap_or(&0) ^ *b.get(i).unwrap_or(&0)) as usize;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_follow_the_declared_agents() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(".kite/tokens");
        let agents: BTreeSet<String> = ["lead".to_string(), "coder".to_string()].into();
        ensure(&dir, &agents).unwrap();
        let lead = read(&dir, "lead").unwrap();
        assert_eq!(lead.len(), 64);
        assert_ne!(lead, read(&dir, "coder").unwrap());
        let mode = fs::metadata(path(&dir, "lead")).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);
        assert!(fs::read_to_string(root.path().join(".kite/.gitignore")).unwrap().contains('*'));

        // Ensuring again keeps tokens; dropping an agent drops its token.
        ensure(&dir, &["lead".to_string()].into()).unwrap();
        assert_eq!(read(&dir, "lead").unwrap(), lead);
        assert!(!path(&dir, "coder").exists());

        rotate(&dir, "lead").unwrap();
        assert_ne!(read(&dir, "lead").unwrap(), lead);
    }

    #[test]
    fn matching_needs_every_byte() {
        assert!(matches("abc", "abc"));
        assert!(!matches("abd", "abc"));
        assert!(!matches("ab", "abc"));
        assert!(!matches("", "abc"));
    }
}
