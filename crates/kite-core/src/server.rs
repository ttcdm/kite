//! The broker's Unix socket. Each connection gets a reader that forwards
//! frames to the broker in arrival order and a writer that sends replies as
//! they complete, so a long recv never holds up the connection's other calls.

use crate::broker::{BrokerHandle, Reply};
use kite_proto::wire::{ClientFrame, MAX_FRAME_BYTES, ServerFrame};
use kite_proto::{ErrorCode, KiteError};
use std::future::Future;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

/// Binds the socket, refusing if another broker is already listening there.
/// The directory is created owner-only and the socket is made owner-only.
pub async fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(dir) = path.parent()
        && !dir.exists()
    {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
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

/// Accepts connections until `stop` resolves, then removes the socket file.
pub async fn serve(listener: UnixListener, path: PathBuf, handle: BrokerHandle, stop: impl Future<Output = ()>) {
    tokio::pin!(stop);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => { tokio::spawn(connection(stream, handle.clone())); }
                Err(e) => tracing::warn!("accept failed: {e}"),
            },
            _ = &mut stop => break,
        }
    }
    let _ = std::fs::remove_file(&path);
}

fn frame(id: u64, reply: Reply) -> ServerFrame {
    let (ok, error) = match reply.result {
        Ok(v) => (Some(v), None),
        Err(e) => (None, Some(e)),
    };
    ServerFrame { id, ok, error, inbox: reply.inbox }
}

async fn connection(stream: UnixStream, handle: BrokerHandle) {
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
    loop {
        buf.clear();
        match (&mut reader).take(MAX_FRAME_BYTES as u64 + 1).read_until(b'\n', &mut buf).await {
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
