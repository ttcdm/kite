//! The broker's Unix socket. Each connection gets a reader that forwards
//! frames to the broker in arrival order and a writer that sends replies as
//! they complete, so a long recv never holds up the connection's other calls.

use crate::broker::{BrokerHandle, Reply};
use kite_proto::wire::{ClientFrame, MAX_FRAME_BYTES, ServerFrame};
use kite_proto::{ErrorCode, KiteError};
use std::future::Future;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Binds the socket, refusing if another broker is already listening there.
/// The directory is created owner-only and the socket is made owner-only.
pub async fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(dir) = path.parent() {
        private_dir(dir)?;
    }
    if path.exists() {
        if UnixStream::connect(path).await.is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("a kite broker is already listening at {}", path.display()),
            ));
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot bind {}: {e} (macOS limits socket paths to 104 bytes)", path.display()),
        )
    })?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Makes `dir` a directory only its owner can enter, so the socket inside it is
/// private from the moment it is bound. A directory owned by someone else is refused.
fn private_dir(dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let meta = std::fs::metadata(dir)?;
    // SAFETY: geteuid has no arguments and cannot fail.
    let me = unsafe { libc::geteuid() };
    if meta.uid() != me {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} belongs to another user; kite needs a directory of its own", dir.display()),
        ));
    }
    if meta.mode() & 0o077 != 0 {
        tracing::warn!(dir = %dir.display(), "the socket directory was open to other users; restricting it to 0700");
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Connection limits; see `[broker] max_connections`.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_connections: usize,
    /// A connection that has not tried to register is closed after this long without a frame.
    pub idle_unregistered: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_connections: 256, idle_unregistered: Duration::from_secs(60) }
    }
}

/// Accepts connections until `stop` resolves, then removes the socket file.
pub async fn serve(
    listener: UnixListener,
    path: PathBuf,
    handle: BrokerHandle,
    limits: Limits,
    stop: impl Future<Output = ()>,
) {
    tokio::pin!(stop);
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => match slots.clone().try_acquire_owned() {
                    Ok(slot) => { tokio::spawn(connection(stream, handle.clone(), limits.idle_unregistered, slot)); }
                    Err(_) => {
                        tracing::warn!(limit = limits.max_connections, "refused a connection: too many open");
                        tokio::spawn(refuse(stream, limits.max_connections));
                    }
                },
                Err(e) => tracing::warn!("accept failed: {e}"),
            },
            _ = &mut stop => break,
        }
    }
    let _ = std::fs::remove_file(&path);
}

/// Tells a connection over the limit why it is being closed.
async fn refuse(mut stream: UnixStream, limit: usize) {
    let e = KiteError::new(ErrorCode::Busy, format!("the broker already has {limit} connections open; retry later"));
    let mut line = serde_json::to_vec(&frame(0, Reply { result: Err(e), inbox: None })).expect("frames serialize");
    line.push(b'\n');
    let _ = stream.write_all(&line).await;
}

fn frame(id: u64, reply: Reply) -> ServerFrame {
    let (ok, error) = match reply.result {
        Ok(v) => (Some(v), None),
        Err(e) => (None, Some(e)),
    };
    ServerFrame { id, ok, error, inbox: reply.inbox }
}

async fn connection(stream: UnixStream, handle: BrokerHandle, idle: Duration, _slot: OwnedSemaphorePermit) {
    let conn = handle.connect();
    let (r, w) = stream.into_split();
    let (out, mut out_rx) = mpsc::channel::<ServerFrame>(256);
    let writer = tokio::spawn(async move {
        let mut w = BufWriter::new(w);
        while let Some(f) = out_rx.recv().await {
            let mut line = serde_json::to_vec(&f).expect("frames serialize");
            line.push(b'\n');
            if w.write_all(&line).await.is_err() {
                break;
            }
            if out_rx.is_empty() && w.flush().await.is_err() {
                break;
            }
        }
        let _ = w.flush().await;
    });

    let mut reader = BufReader::new(r);
    let mut buf = Vec::new();
    let mut tried_register = false;
    loop {
        buf.clear();
        let mut limited = (&mut reader).take(MAX_FRAME_BYTES as u64 + 1);
        let read = limited.read_until(b'\n', &mut buf);
        let read = if tried_register {
            read.await
        } else {
            match tokio::time::timeout(idle, read).await {
                Ok(r) => r,
                Err(_) => {
                    let e = KiteError::new(ErrorCode::InvalidFrame, "closing an idle connection that never registered");
                    let _ = out.send(frame(0, Reply { result: Err(e), inbox: None })).await;
                    break;
                }
            }
        };
        match read {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if buf.len() > MAX_FRAME_BYTES {
            let e = KiteError::new(ErrorCode::InvalidFrame, format!("frame longer than {MAX_FRAME_BYTES} bytes"));
            let _ = out.send(frame(0, Reply { result: Err(e), inbox: None })).await;
            break;
        }
        let line = buf.trim_ascii();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<ClientFrame>(line) {
            Ok(f) => {
                tried_register |= matches!(f.op, kite_proto::wire::Op::Register { .. });
                // Submitting here, before spawning, keeps the connection's operations in order.
                let rx = handle.submit(conn, f.op).await;
                let out = out.clone();
                tokio::spawn(async move {
                    let reply = rx.await.unwrap_or_else(|_| Reply::shutting_down());
                    let _ = out.send(frame(f.id, reply)).await;
                });
            }
            Err(e) => {
                let id = serde_json::from_slice::<serde_json::Value>(line)
                    .ok()
                    .and_then(|v| v.get("id").and_then(|i| i.as_u64()))
                    .unwrap_or(0);
                let e = KiteError::new(ErrorCode::InvalidFrame, format!("not a kite frame: {e}"));
                let _ = out.send(frame(id, Reply { result: Err(e), inbox: None })).await;
            }
        }
    }
    handle.disconnect(conn).await;
    drop(out);
    let _ = writer.await;
}
