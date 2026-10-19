//! Append-only, hash-chained record of everything the broker accepted, refused
//! and decided. A dedicated OS thread owns the file so disk latency never
//! blocks the broker's event loop; the bounded channel in front of it applies
//! backpressure if the disk falls behind.
//!
//! The ledger is a directory of segments, `000001.jsonl`, `000002.jsonl`, …;
//! the highest is live. One hash chain runs through all of them. When `kite up`
//! finds the live segment larger than `segment_bytes`, it starts the next one
//! with a single `snapshot` record holding the replay state (see `restore.rs`),
//! so a start reads only the live segment, however long the history.

use crate::restore::{Fold, Restored};
use kite_proto::config::Fsync;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const QUEUE: usize = 4096;
/// The ledger format this kite writes; `kite up` refuses a ledger from a newer one.
pub const FORMAT: u64 = 2;
/// Default size past which the next start begins a new segment.
pub const SEGMENT_BYTES: u64 = 64 << 20;

/// The segment directory for a `broker.ledger` setting. A `.jsonl` path names the
/// single-file ledger of Kite 0.1 and maps to the directory of the same stem.
pub fn segment_dir(setting: &Path) -> PathBuf {
    if setting.extension().is_some_and(|e| e == "jsonl") { setting.with_extension("") } else { setting.to_path_buf() }
}

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{index:06}.jsonl"))
}

/// The segments in `dir`, oldest first.
pub fn segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "jsonl")
            && let Some(index) = path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse::<u64>().ok())
        {
            out.push((index, path));
        }
    }
    out.sort();
    Ok(out)
}

/// The files that make up the ledger at `path`, oldest first: a single file, or a segment directory.
fn ledger_files(path: &Path) -> io::Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    Ok(segments(&segment_dir(path))?.into_iter().map(|(_, p)| p).collect())
}

/// The last record of a file, read from the end.
fn last_record(path: &Path) -> io::Result<Option<Value>> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut chunk = 64 * 1024;
    loop {
        let start = len.saturating_sub(chunk);
        f.seek(SeekFrom::Start(start))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let body = buf.strip_suffix(b"\n").unwrap_or(&buf);
        match body.iter().rposition(|&b| b == b'\n') {
            Some(i) => return Ok(serde_json::from_slice(&body[i + 1..]).ok()),
            None if start == 0 => return Ok(serde_json::from_slice(body).ok()),
            None => chunk *= 2,
        }
    }
}

enum Msg {
    Append(Value),
    Flush(oneshot::Sender<io::Result<()>>),
}

pub struct Ledger {
    tx: Option<mpsc::Sender<Msg>>,
    thread: Option<JoinHandle<io::Result<()>>>,
    /// The live segment.
    path: PathBuf,
    recovered: Option<u64>,
    /// What the history restores, for the broker to take once.
    history: Option<Restored>,
    /// Held for the ledger's lifetime: one broker per ledger.
    _lock: File,
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

/// One file's chain, as far as the file itself can say.
struct Scanned {
    first: Option<(u64, String)>,
    last_n: u64,
    last_hash: String,
}

/// Checks one file's internal chain, handing each record to `each`.
fn scan(path: &Path, mut each: impl FnMut(Value)) -> Result<Scanned, VerifyError> {
    let f = File::open(path).map_err(|e| VerifyError { line: 0, reason: format!("{}: {e}", path.display()) })?;
    let mut s = Scanned { first: None, last_n: 0, last_hash: String::new() };
    for (i, line) in BufReader::new(f).lines().enumerate() {
        let lineno = i as u64 + 1;
        let err = |reason: String| VerifyError { line: lineno, reason: format!("{}: {reason}", path.display()) };
        let line = line.map_err(|e| err(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: Value = serde_json::from_str(&line).map_err(|e| err(format!("not JSON: {e}")))?;
        let n = rec["n"].as_u64().ok_or_else(|| err("missing n".into()))?;
        let ts = rec["ts_ms"].as_u64().ok_or_else(|| err("missing ts_ms".into()))?;
        let rprev = rec["prev"].as_str().ok_or_else(|| err("missing prev".into()))?.to_string();
        let hash = rec["hash"].as_str().ok_or_else(|| err("missing hash".into()))?.to_string();
        match &s.first {
            None => s.first = Some((n, rprev.clone())),
            Some(_) => {
                if n != s.last_n + 1 {
                    return Err(err(format!("expected record {}, found {n}", s.last_n + 1)));
                }
                if rprev != s.last_hash {
                    return Err(err("prev does not match the previous record's hash".into()));
                }
            }
        }
        if record_hash(&rprev, n, ts, &rec["event"]) != hash {
            return Err(err("hash does not match the record's contents".into()));
        }
        s.last_n = n;
        s.last_hash = hash;
        each(rec);
    }
    Ok(s)
}

/// Verifies a whole ledger: each segment's chain and each segment's link to the
/// one before it. If the oldest segments were deleted by hand, verification
/// starts from the oldest one present.
pub fn verify(path: &Path) -> Result<VerifyReport, VerifyError> {
    let files = ledger_files(path).map_err(|e| VerifyError { line: 0, reason: e.to_string() })?;
    if files.is_empty() {
        return Err(VerifyError { line: 0, reason: format!("no ledger at {}", path.display()) });
    }
    let mut records = 0u64;
    let mut before: Option<(u64, String)> = None;
    for (k, file) in files.iter().enumerate() {
        let s = scan(file, |_| records += 1)?;
        if let Some((n, prev)) = &s.first {
            let starts_history = k == 0 && (path.is_file() || file.file_stem().is_some_and(|stem| stem == "000001"));
            let expected = match &before {
                Some((last_n, last_hash)) => Some((last_n + 1, last_hash.clone())),
                None if starts_history => Some((1, GENESIS.to_string())),
                None => None,
            };
            if let Some((en, eprev)) = expected
                && (*n != en || *prev != eprev)
            {
                return Err(VerifyError {
                    line: 1,
                    reason: format!("{}: does not continue the chain before it", file.display()),
                });
            }
            before = Some((s.last_n, s.last_hash.clone()));
        }
    }
    let last_hash = before.map(|(_, h)| h).unwrap_or_else(|| GENESIS.to_string());
    Ok(VerifyReport { records, last_hash })
}

/// Every record of the ledger at `path`, oldest first, across segments.
pub fn read_all_records(path: &Path) -> io::Result<Vec<Value>> {
    let mut out = Vec::new();
    for file in ledger_files(path)? {
        for line in BufReader::new(File::open(file)?).lines() {
            if let Ok(rec) = serde_json::from_str::<Value>(&line?) {
                out.push(rec);
            }
        }
    }
    Ok(out)
}

/// Every event of the ledger at `path`, oldest first, across segments.
pub fn read_all_events(path: &Path) -> io::Result<Vec<Value>> {
    Ok(read_all_records(path)?.into_iter().map(|mut r| r["event"].take()).collect())
}

impl Ledger {
    /// Opens (or creates) the ledger and continues its chain, compacting it if
    /// the live segment is over the default size. See [`Ledger::open_with`].
    pub fn open(path: &Path, fsync: Fsync) -> io::Result<Ledger> {
        Ledger::open_with(path, fsync, SEGMENT_BYTES)
    }

    /// Opens the ledger at `setting` (a segment directory, or a Kite 0.1 ledger
    /// file, which is moved into one as segment 1):
    ///
    /// 1. Takes the directory's lock, so a second broker fails here.
    /// 2. Repairs a torn final record in the live segment.
    /// 3. Verifies the live segment's chain and its link to the segment before it.
    /// 4. Replays it (from its snapshot, if it starts with one).
    /// 5. If it is larger than `segment_bytes`, starts the next segment with a
    ///    snapshot of that state (written to a temp file, synced, then renamed).
    pub fn open_with(setting: &Path, fsync: Fsync, segment_bytes: u64) -> io::Result<Ledger> {
        let dir = segment_dir(setting);
        std::fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("LOCK"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{} is in use by another kite broker", dir.display()),
                ));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
        let legacy = dir.with_extension("jsonl");
        if legacy.is_file() && segments(&dir)?.is_empty() {
            std::fs::rename(&legacy, segment_path(&dir, 1))?;
        }
        // A compaction interrupted before its rename leaves only a temp file behind.
        for entry in std::fs::read_dir(&dir)? {
            let p = entry?.path();
            if p.extension().is_some_and(|e| e == "tmp") {
                std::fs::remove_file(p)?;
            }
        }
        let segs = segments(&dir)?;
        let (index, mut live) = segs.last().cloned().unwrap_or((1, segment_path(&dir, 1)));
        let mut file = OpenOptions::new().create(true).append(true).open(&live)?;
        let recovered = repair_tail(&live, &mut file)?;

        let bad = |what: String| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{what}; move {} aside to start a new ledger", dir.display()),
            )
        };
        let mut events = Vec::new();
        let s = scan(&live, |mut r| events.push(r["event"].take())).map_err(|e| bad(e.to_string()))?;
        if let Some((first_n, first_prev)) = &s.first {
            let before = segs.len().checked_sub(2).map(|k| &segs[k].1);
            let expected = match before {
                Some(p) => last_record(p)?
                    .map(|r| (r["n"].as_u64().unwrap_or(0) + 1, r["hash"].as_str().unwrap_or("").to_string())),
                None if index == 1 => Some((1, GENESIS.to_string())),
                None => None,
            };
            if let Some((n, prev)) = expected
                && (*first_n != n || *first_prev != prev)
            {
                return Err(bad(format!("{} does not continue the segment before it", live.display())));
            }
        }
        if let Some(f) = events.iter().filter_map(|e| e["format"].as_u64()).max()
            && f > FORMAT
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} was written by a newer kite (ledger format {f}; this kite reads up to {FORMAT})",
                    dir.display()
                ),
            ));
        }
        let mut fold = Fold::default();
        for (i, e) in events.iter().enumerate() {
            if i == 0 && e["type"] == "snapshot" {
                fold = serde_json::from_value(e["state"].clone())
                    .map_err(|err| bad(format!("{}: unreadable snapshot: {err}", live.display())))?;
            } else {
                fold.apply(e);
            }
        }
        let history = fold.clone().finish();
        let (mut n, mut prev) = if s.first.is_some() { (s.last_n, s.last_hash) } else { (0, GENESIS.to_string()) };

        if file.metadata()?.len() > segment_bytes {
            let next = segment_path(&dir, index + 1);
            let tmp = next.with_extension("tmp");
            let event = json!({ "type": "snapshot", "format": FORMAT, "state": fold.compacted() });
            n += 1;
            let ts = now_ms();
            let hash = record_hash(&prev, n, ts, &event);
            let rec = json!({ "n": n, "ts_ms": ts, "prev": prev, "event": event, "hash": hash });
            {
                let mut out = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
                serde_json::to_writer(&mut out, &rec)?;
                out.write_all(b"\n")?;
                out.sync_all()?;
            }
            std::fs::rename(&tmp, &next)?;
            File::open(&dir)?.sync_all()?;
            prev = hash;
            live = next;
            file = OpenOptions::new().append(true).open(&live)?;
        }

        let (tx, rx) = mpsc::channel(QUEUE);
        let thread =
            std::thread::Builder::new().name("kite-ledger".into()).spawn(move || writer(file, rx, n, prev, fsync))?;
        Ok(Ledger { tx: Some(tx), thread: Some(thread), path: live, recovered, history: Some(history), _lock: lock })
    }

    /// What the history restores. The broker takes it once, at start.
    pub fn take_history(&mut self) -> Restored {
        self.history.take().unwrap_or_default()
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

    /// The first segment of the ledger set at `p`.
    fn live(p: &Path) -> PathBuf {
        segment_path(&segment_dir(p), 1)
    }

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
        let clean = std::fs::read(live(&p)).unwrap();
        // A crash halfway through the next record.
        let torn: &[u8] = b"{\"n\":2,\"ts_ms\":1,\"prev\":\"ab";
        std::fs::write(live(&p), [clean.as_slice(), torn].concat()).unwrap();
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
        let mut bytes = std::fs::read(live(&p)).unwrap();
        bytes.pop();
        std::fs::write(live(&p), bytes).unwrap();
        let l = Ledger::open(&p, Fsync::Batch).unwrap();
        assert_eq!(l.recovered(), Some(0));
        l.append(json!({"type": "b"})).await.unwrap();
        l.close().unwrap();
        assert_eq!(verify(&p).unwrap().records, 2);
    }

    fn first_event(path: &Path) -> Value {
        let line = std::fs::read_to_string(path).unwrap().lines().next().unwrap().to_string();
        serde_json::from_str::<Value>(&line).unwrap()["event"].clone()
    }

    async fn write_events(p: &Path, bytes: u64, events: &[Value]) {
        let l = Ledger::open_with(p, Fsync::Batch, bytes).unwrap();
        for e in events {
            l.append(e.clone()).await.unwrap();
        }
        l.close().unwrap();
    }

    #[tokio::test]
    async fn an_oversized_segment_is_compacted_into_a_snapshot_at_the_next_open() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ledger");
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "started"}), json!({"type": "note", "event": {}})]).await;
        // Tiny segments: every open compacts.
        write_events(&p, 1, &[json!({"type": "note", "event": {"x": 1}})]).await;
        let segs = segments(&p).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(first_event(&segs[1].1)["type"], "snapshot");
        assert_eq!(first_event(&segs[1].1)["format"], FORMAT);
        let report = verify(&p).unwrap();
        assert_eq!(report.records, 4, "2 + snapshot + 1, one chain across both segments");
        // A large limit leaves the live segment alone.
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "note", "event": {}})]).await;
        assert_eq!(segments(&p).unwrap().len(), 2);
        assert_eq!(verify(&p).unwrap().records, 5);
    }

    #[tokio::test]
    async fn an_interrupted_compaction_leaves_the_old_segment_live() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ledger");
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "a"})]).await;
        std::fs::write(p.join("000002.tmp"), b"{\"n\":2,\"half").unwrap();
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "b"})]).await;
        assert!(!p.join("000002.tmp").exists());
        assert_eq!(segments(&p).unwrap().len(), 1);
        assert_eq!(verify(&p).unwrap().records, 2);
    }

    #[tokio::test]
    async fn a_single_file_ledger_from_kite_0_1_is_moved_into_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ledger");
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "a"}), json!({"type": "b"})]).await;
        let old = dir.path().join("ledger.jsonl");
        std::fs::rename(p.join("000001.jsonl"), &old).unwrap();
        std::fs::remove_dir_all(&p).unwrap();
        // The old default setting and the new one both find it.
        write_events(&old, SEGMENT_BYTES, &[json!({"type": "c"})]).await;
        assert!(!old.exists() && p.join("000001.jsonl").exists());
        assert_eq!(verify(&p).unwrap().records, 3);
    }

    #[tokio::test]
    async fn old_segments_are_checked_by_verify_and_links_at_start() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ledger");
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "a", "i": 1}), json!({"type": "b"})]).await;
        write_events(&p, 1, &[]).await;
        // Editing a closed segment does not stop a start (only the live one is read) ...
        let first = p.join("000001.jsonl");
        let text = std::fs::read_to_string(&first).unwrap().replace("\"i\":1", "\"i\":2");
        std::fs::write(&first, text).unwrap();
        write_events(&p, SEGMENT_BYTES, &[]).await;
        // ... but verification finds it.
        assert!(verify(&p).unwrap_err().reason.contains("000001.jsonl"));
        // A live segment that does not continue the one before it refuses to start.
        let live = p.join("000002.jsonl");
        let text = std::fs::read_to_string(&live).unwrap();
        let mut rec: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        rec["prev"] = json!(GENESIS);
        rec["hash"] =
            json!(record_hash(GENESIS, rec["n"].as_u64().unwrap(), rec["ts_ms"].as_u64().unwrap(), &rec["event"]));
        let rest: Vec<&str> = text.lines().skip(1).collect();
        std::fs::write(&live, format!("{rec}\n{}", rest.iter().map(|l| format!("{l}\n")).collect::<String>())).unwrap();
        let e = Ledger::open(&p, Fsync::Batch).err().expect("a broken link refuses to start");
        assert!(e.to_string().contains("does not continue"), "{e}");
    }

    #[tokio::test]
    async fn a_ledger_from_a_newer_kite_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ledger");
        write_events(&p, SEGMENT_BYTES, &[json!({"type": "started", "format": FORMAT + 1})]).await;
        let e = Ledger::open(&p, Fsync::Batch).err().expect("refused");
        assert!(e.to_string().contains("newer kite"), "{e}");
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
        let text = std::fs::read_to_string(live(&p)).unwrap().replace("\"i\":1", "\"i\":7");
        std::fs::write(live(&p), text).unwrap();
        assert_eq!(verify(&p).unwrap_err().line, 2);
        assert!(Ledger::open(&p, Fsync::Batch).is_err());
    }
}
