//! Append-only, hash-chained record of everything the broker accepted, refused
//! and decided. A dedicated OS thread owns the file so disk latency never
//! blocks the broker's event loop; the bounded channel in front of it applies
//! backpressure if the disk falls behind.

use kite_proto::config::Fsync;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const QUEUE: usize = 4096;

enum Msg {
    Append(Value),
    Flush(oneshot::Sender<io::Result<()>>),
}

pub struct Ledger {
    tx: Option<mpsc::Sender<Msg>>,
    thread: Option<JoinHandle<io::Result<()>>>,
    path: PathBuf,
    recovered: Option<u64>,
}

/// Repairs a final line left incomplete by a crash mid-write. A complete
/// record whose newline was lost gets the newline back; anything else after
/// the last newline is cut off. Returns the bytes dropped, or None if the
/// file already ended cleanly. Complete lines are never touched, so a bad
/// hash still fails verification.
fn repair_tail(path: &Path, file: &mut File) -> io::Result<Option<u64>> {
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() || bytes.ends_with(b"\n") {
        return Ok(None);
    }
    let start = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    let tail = &bytes[start..];
    let complete = serde_json::from_slice::<Value>(tail).is_ok_and(|v| v.get("hash").is_some());
    if complete {
        file.write_all(b"\n")?;
        file.sync_data()?;
        return Ok(Some(0));
    }
    file.set_len(start as u64)?;
    file.sync_data()?;
    Ok(Some(tail.len() as u64))
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// The hash of a record: sha256 over the previous hash and the record body.
pub fn record_hash(prev: &str, n: u64, ts_ms: u64, event: &Value) -> String {
    let body = json!({ "n": n, "ts_ms": ts_ms, "event": event });
    let mut h = Sha256::new();
    h.update(prev.as_bytes());
    h.update(b"\n");
    h.update(serde_json::to_string(&body).expect("json values serialize").as_bytes());
    hex::encode(h.finalize())
}

#[derive(Debug, Clone, PartialEq)]
pub struct VerifyReport {
    pub records: u64,
    pub last_hash: String,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("ledger line {line}: {reason}")]
pub struct VerifyError {
    pub line: u64,
    pub reason: String,
}

/// Reads the whole ledger and recomputes the chain. Any edit, deletion or
/// reordering of a record breaks it at that line.
pub fn verify(path: &Path) -> Result<VerifyReport, VerifyError> {
    let f = File::open(path).map_err(|e| VerifyError { line: 0, reason: e.to_string() })?;
    let mut prev = GENESIS.to_string();
    let mut records = 0u64;
    for (i, line) in BufReader::new(f).lines().enumerate() {
        let lineno = i as u64 + 1;
        let err = |reason: String| VerifyError { line: lineno, reason };
        let line = line.map_err(|e| err(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: Value = serde_json::from_str(&line).map_err(|e| err(format!("not JSON: {e}")))?;
        let n = rec["n"].as_u64().ok_or_else(|| err("missing n".into()))?;
        let ts = rec["ts_ms"].as_u64().ok_or_else(|| err("missing ts_ms".into()))?;
        let rprev = rec["prev"].as_str().ok_or_else(|| err("missing prev".into()))?;
        let hash = rec["hash"].as_str().ok_or_else(|| err("missing hash".into()))?;
        if n != records + 1 {
            return Err(err(format!("expected record {}, found {n}", records + 1)));
        }
        if rprev != prev {
            return Err(err("prev does not match the previous record's hash".into()));
        }
        if record_hash(&prev, n, ts, &rec["event"]) != hash {
            return Err(err("hash does not match the record's contents".into()));
        }
        prev = hash.to_string();
        records = n;
    }
    Ok(VerifyReport { records, last_hash: prev })
}

/// Every event in the ledger, in order. Call after `Ledger::open` has verified the chain.
pub fn read_events(path: &Path) -> io::Result<Vec<Value>> {
    let f = File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if let Ok(mut rec) = serde_json::from_str::<Value>(&line) {
            out.push(rec["event"].take());
        }
    }
    Ok(out)
}

impl Ledger {
    /// Opens (or creates) the ledger and continues its chain. Refuses to start
    /// on a ledger whose chain does not verify.
    ///
    /// The file stays exclusively locked while the ledger is open, so a second
    /// broker on the same project fails here instead of interleaving records
    /// with the first and breaking the chain.
    pub fn open(path: &Path, fsync: Fsync) -> io::Result<Ledger> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{} is in use by another kite broker", path.display()),
                ));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
        let recovered = repair_tail(path, &mut file)?;
        let (n, prev) = if std::fs::metadata(path)?.len() > 0 {
            let r = verify(path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {e}; move it aside to start a new ledger", path.display()),
                )
            })?;
            (r.records, r.last_hash)
        } else {
            (0, GENESIS.to_string())
        };
        let (tx, rx) = mpsc::channel(QUEUE);
        let thread =
            std::thread::Builder::new().name("kite-ledger".into()).spawn(move || writer(file, rx, n, prev, fsync))?;
        Ok(Ledger { tx: Some(tx), thread: Some(thread), path: path.to_path_buf(), recovered })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes of a torn final record that `open` cut off (0 if it only restored a
    /// missing newline), or None if the ledger ended cleanly.
    pub fn recovered(&self) -> Option<u64> {
        self.recovered
    }

    pub async fn append(&self, event: Value) -> io::Result<()> {
        let tx = self.tx.as_ref().ok_or_else(|| io::Error::other("ledger closed"))?;
        tx.send(Msg::Append(event)).await.map_err(|_| io::Error::other("ledger writer stopped"))
    }

    /// Returns once everything appended so far is written and fsynced.
    pub async fn flush(&self) -> io::Result<()> {
        let tx = self.tx.as_ref().ok_or_else(|| io::Error::other("ledger closed"))?;
        let (otx, orx) = oneshot::channel();
        tx.send(Msg::Flush(otx)).await.map_err(|_| io::Error::other("ledger writer stopped"))?;
        orx.await.map_err(|_| io::Error::other("ledger writer stopped"))?
    }

    pub fn close(mut self) -> io::Result<()> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.tx.take();
        match self.thread.take() {
            Some(t) => t.join().map_err(|_| io::Error::other("ledger writer panicked"))?,
            None => Ok(()),
        }
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn writer(file: File, mut rx: mpsc::Receiver<Msg>, mut n: u64, mut prev: String, fsync: Fsync) -> io::Result<()> {
    let mut out = BufWriter::new(file);
    while let Some(first) = rx.blocking_recv() {
        let mut next = Some(first);
        // Drain whatever is queued, then sync once (Fsync::Batch).
        while let Some(msg) = next.take() {
            match msg {
                Msg::Append(event) => {
                    n += 1;
                    let ts = now_ms();
                    let hash = record_hash(&prev, n, ts, &event);
                    let rec = json!({ "n": n, "ts_ms": ts, "prev": prev, "event": event, "hash": hash });
                    serde_json::to_writer(&mut out, &rec)?;
                    out.write_all(b"\n")?;
                    prev = hash;
                    if fsync == Fsync::Always {
                        out.flush()?;
                        out.get_ref().sync_data()?;
                    }
                }
                Msg::Flush(done) => {
                    let r = out.flush().and_then(|_| out.get_ref().sync_data());
                    let _ = done.send(r);
                }
            }
            next = rx.try_recv().ok();
        }
        out.flush()?;
        out.get_ref().sync_data()?;
    }
    out.flush()?;
    out.get_ref().sync_data()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn chain_verifies_and_continues_across_opens() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("l.jsonl");
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        l.append(json!({"type": "a"})).await.unwrap();
        l.append(json!({"type": "b", "z": 1, "a": [1, 2]})).await.unwrap();
        l.close().unwrap();
        let l = Ledger::open(&p, Fsync::Always).unwrap();
        l.append(json!({"type": "c"})).await.unwrap();
        l.flush().await.unwrap();
        l.close().unwrap();
        assert_eq!(verify(&p).unwrap().records, 3);
    }

    #[tokio::test]
    async fn a_second_writer_is_refused_while_the_first_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("l.jsonl");
        let first = Ledger::open(&p, Fsync::Batch).unwrap();
        let second = Ledger::open(&p, Fsync::Batch).err().expect("the ledger is locked");
        assert_eq!(second.kind(), io::ErrorKind::AddrInUse);
        first.append(json!({"type": "a"})).await.unwrap();
        first.close().unwrap();
        // Closing releases the lock.
        Ledger::open(&p, Fsync::Batch).unwrap().close().unwrap();
        assert_eq!(verify(&p).unwrap().records, 1);
    }

    #[tokio::test]
    async fn a_torn_final_record_is_cut_off_and_reported() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("l.jsonl");
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        l.append(json!({"type": "a"})).await.unwrap();
        l.close().unwrap();
        let clean = std::fs::read(&p).unwrap();
        // A crash halfway through the next record.
        let torn: &[u8] = b"{\"n\":2,\"ts_ms\":1,\"prev\":\"ab";
        std::fs::write(&p, [clean.as_slice(), torn].concat()).unwrap();
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        assert_eq!(l.recovered(), Some(torn.len() as u64));
        l.append(json!({"type": "b"})).await.unwrap();
        l.close().unwrap();
        assert_eq!(verify(&p).unwrap().records, 2);
    }

    #[tokio::test]
    async fn a_complete_record_missing_its_newline_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("l.jsonl");
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        l.append(json!({"type": "a"})).await.unwrap();
        l.close().unwrap();
        let mut bytes = std::fs::read(&p).unwrap();
        bytes.pop();
        std::fs::write(&p, bytes).unwrap();
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        assert_eq!(l.recovered(), Some(0));
        l.append(json!({"type": "b"})).await.unwrap();
        l.close().unwrap();
        assert_eq!(verify(&p).unwrap().records, 2);
    }

    #[tokio::test]
    async fn tampering_is_detected_at_the_edited_line() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("l.jsonl");
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        for i in 0..3 {
            l.append(json!({"type": "x", "i": i})).await.unwrap();
        }
        l.close().unwrap();
        let text = std::fs::read_to_string(&p).unwrap().replace("\"i\":1", "\"i\":7");
        std::fs::write(&p, text).unwrap();
        assert_eq!(verify(&p).unwrap_err().line, 2);
        assert!(Ledger::open(&p, Fsync::Batch).is_err());
    }
}
