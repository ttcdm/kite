//! Finding kite.toml and the broker socket, and naming lease resources.

use anyhow::{Context, bail};
use kite_proto::Config;
use std::path::{Component, Path, PathBuf};

pub struct Paths {
    pub config: PathBuf,
    /// Directory holding kite.toml.
    pub root: PathBuf,
    pub socket: PathBuf,
}

/// `--config`, else `KITE_CONFIG`, else the nearest kite.toml at or above the
/// current directory. `KITE_SOCKET` overrides the socket path (the supervisor
/// sets it for agents it launches).
pub fn find(explicit: Option<&Path>) -> anyhow::Result<(Paths, Config)> {
    let config = match explicit {
        Some(p) => p.to_path_buf(),
        None => {
            let mut dir = std::env::current_dir()?;
            loop {
                let candidate = dir.join("kite.toml");
                if candidate.exists() {
                    break candidate;
                }
                if !dir.pop() {
                    bail!("no kite.toml here or in any parent directory (run `kite init`, or pass --config)");
                }
            }
        }
    };
    let mut parsed = Config::load(&config).map_err(anyhow::Error::msg)?;
    let socket = std::env::var_os("KITE_SOCKET").map(PathBuf::from).unwrap_or_else(|| parsed.broker.socket.clone());
    parsed.broker.socket = socket.clone();
    let config = config.canonicalize().with_context(|| format!("resolving {}", config.display()))?;
    Ok((Paths { root: parsed.root.clone(), socket, config }, parsed))
}

fn lexical_normalize(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

/// The lease key for `s`. Paths inside the project become root-relative with
/// forward slashes, so `./src/a.rs`, `src/a.rs` and `/abs/proj/src/a.rs` are
/// the same lease. Anything else is used as given.
pub fn resource_key(root: &Path, s: &str) -> String {
    let s = s.trim();
    let looks_like_path = s.contains('/') || s.starts_with('.') || Path::new(s).extension().is_some();
    if !looks_like_path {
        return s.to_string();
    }
    let p = Path::new(s);
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir().map(|d| d.join(p)).unwrap_or_else(|_| p.to_path_buf())
    };
    let abs = abs.canonicalize().unwrap_or_else(|_| lexical_normalize(&abs));
    match abs.strip_prefix(root) {
        Ok(rel) => rel.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/"),
        Err(_) => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_paths_share_one_key() {
        let root = Path::new("/proj");
        assert_eq!(resource_key(root, "/proj/src/../src/a.rs"), "src/a.rs");
        assert_eq!(resource_key(root, "/elsewhere/a.rs"), "/elsewhere/a.rs");
        assert_eq!(resource_key(root, "release-notes"), "release-notes");
    }
}
