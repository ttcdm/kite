//! The broker behind its real Unix socket: the client library, malformed
//! frames, the task-graph runner and the process supervisor.

use kite_core::runner::{self, Plan, TaskState};
use kite_core::supervisor::Supervisor;
use kite_core::{Broker, BrokerHandle, Client, Ledger, server};
use kite_proto::wire::*;
use kite_proto::*;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::oneshot;

const CONFIG: &str = r#"
[schemas."work@1"]
kind = "request"
response = "work.done@1"
schema = { type = "object", required = ["text"], properties = { text = { type = "string" } } }

[schemas."work.done@1"]
kind = "response"
schema = { type = "object", required = ["text"] }

[schemas."tick@1"]
kind = "event"
schema = { type = "object", required = ["sender", "n"] }

[agents.lead]
[agents.worker]
[agents.sink]
[agents.s0]
[agents.s1]
[agents.s2]
[agents.s3]
[agents.s4]
[agents.s5]
[agents.s6]
[agents.s7]
[agents.crashy]
command = ["sh", "-c", "echo started {agent}; exit 3"]
restart = "on-failure"
max_restarts = 2

[[routes]]
from = "lead"
to = "worker"
schemas = ["work@1"]

[[routes]]
from = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"]
to = "sink"
schemas = ["tick@1"]
"#;

struct Stack {
    dir: TempDir,
    socket: PathBuf,
    config: Arc<Config>,
    handle: BrokerHandle,
    stop: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    broker: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn start() -> Stack {
    start_with(server::Limits::default()).await
}

async fn start_with(limits: server::Limits) -> Stack {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(Config::parse(CONFIG, &dir.path().canonicalize().unwrap()).unwrap());
    let ledger = Ledger::open(&config.broker.ledger, config.broker.fsync).unwrap();
    let (handle, broker) = Broker::spawn(config.clone(), ledger);
    let socket = config.broker.socket.clone();
    let listener = server::bind(&socket).await.unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(server::serve(listener, socket.clone(), handle.clone(), limits, async {
        let _ = stopped.await;
    }));
    Stack { dir, socket, config, handle, stop: Some(stop), server, broker }
}

impl Stack {
    async fn agent(&self, name: &str) -> Arc<Client> {
        let c = Client::connect(&self.socket).await.unwrap();
        c.register(name).await.unwrap();
        Arc::new(c)
    }

    async fn shutdown(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.server.await.unwrap();
        self.handle.shutdown().await;
        self.broker.await.unwrap().unwrap();
        assert!(!self.socket.exists(), "the socket file is removed on shutdown");
        kite_core::ledger::verify(&self.config.broker.ledger).unwrap();
        drop(self.dir);
    }
}

fn work(text: &str) -> Draft {
    Draft {
        id: None,
        to: "worker".into(),
        schema: "work@1".parse().unwrap(),
        kind: None,
        payload: json!({ "text": text }),
        timeout_ms: Some(30_000),
        blocking: false,
    }
}

/// A worker that upper-cases each request's text, or refuses `"fail"`.
fn spawn_worker(worker: Arc<Client>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok(batch) = worker.recv(30_000, 8).await else { return };
            for m in batch.messages {
                if m.kind != Kind::Request {
                    continue;
                }
                let text = m.payload["text"].as_str().unwrap_or_default().to_string();
                if text == "fail" {
                    let e = AgentError { code: "refused".into(), message: "asked to fail".into(), detail: None };
                    worker.respond_error(m.id, e).await.unwrap();
                } else {
                    worker.respond(m.id, json!({ "text": text.to_uppercase() })).await.unwrap();
                }
            }
        }
    })
}

#[tokio::test]
async fn client_round_trip_over_the_socket() {
    let s = start().await;
    let lead = s.agent("lead").await;
    let w = spawn_worker(s.agent("worker").await);

    let (sent, got) = lead.request(work("hello"), Duration::from_secs(20)).await.unwrap();
    let AwaitOk::Done { response } = got else { panic!("expected an answer") };
    assert_eq!(response.correlation_id, Some(sent.id));
    assert_eq!(response.payload, json!({ "text": "HELLO" }));

    // A second connection cannot claim an agent that is already connected.
    let dup = Client::connect(&s.socket).await.unwrap();
    assert_eq!(dup.register("lead").await.unwrap_err().code, ErrorCode::AlreadyConnected);

    // One client can have several calls in flight: a parked recv does not block a send.
    let parked = {
        let lead = lead.clone();
        tokio::spawn(async move { lead.recv(5_000, 1).await })
    };
    let (_, got) = lead.request(work("again"), Duration::from_secs(20)).await.unwrap();
    assert!(matches!(got, AwaitOk::Done { .. }));
    parked.abort();
    w.abort();
    drop(lead);
    s.shutdown().await;
}

async fn raw_exchange(socket: &PathBuf, line: &[u8]) -> Option<Value> {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    stream.write_all(line).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut reply = String::new();
    reader.read_line(&mut reply).await.unwrap();
    serde_json::from_str(&reply).ok()
}

#[tokio::test]
async fn malformed_and_oversized_frames_are_refused() {
    let s = start().await;
    let r = raw_exchange(&s.socket, b"{\"id\": 9, \"op\": \"shout\"}\n").await.unwrap();
    assert_eq!((r["id"].as_u64(), r["error"]["code"].as_str()), (Some(9), Some("invalid_frame")));
    let r = raw_exchange(&s.socket, b"not json\n").await.unwrap();
    assert_eq!(r["error"]["code"], "invalid_frame");
    let mut huge = vec![b'x'; MAX_FRAME_BYTES + 10];
    huge.push(b'\n');
    let r = raw_exchange(&s.socket, &huge).await.unwrap();
    assert!(r["error"]["message"].as_str().unwrap().contains("longer than"));
    s.shutdown().await;
}

#[tokio::test]
async fn a_second_broker_cannot_take_a_live_socket() {
    let s = start().await;
    let err = server::bind(&s.socket).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    s.shutdown().await;
}

#[tokio::test]
async fn the_runner_follows_the_graph_and_skips_after_failures() {
    let s = start().await;
    let lead = s.agent("lead").await;
    let w = spawn_worker(s.agent("worker").await);
    let plan = Plan::parse(
        r#"
        as = "lead"
        max_parallel = 2
        [[tasks]]
        id = "a"
        to = "worker"
        schema = "work@1"
        payload = { text = "first" }
        [[tasks]]
        id = "b"
        to = "worker"
        schema = "work@1"
        after = ["a"]
        payload = { text = "${tasks.a.result.text} then second" }
        [[tasks]]
        id = "bad"
        to = "worker"
        schema = "work@1"
        payload = { text = "fail" }
        [[tasks]]
        id = "after_bad"
        to = "worker"
        schema = "work@1"
        after = ["bad"]
        payload = { text = "never" }
        "#,
    )
    .unwrap();
    let report = runner::run(lead.clone(), &plan).await.unwrap();
    assert!(!report.ok);
    let t = &report.tasks;
    assert_eq!(t["a"].state, TaskState::Done);
    assert_eq!(t["b"].result, Some(json!({ "text": "FIRST THEN SECOND" })));
    assert_eq!(t["bad"].state, TaskState::Failed);
    assert_eq!(t["bad"].error.as_ref().unwrap()["code"], "refused");
    assert_eq!(t["after_bad"].state, TaskState::Skipped);
    w.abort();
    s.shutdown().await;
}

#[tokio::test]
async fn the_supervisor_restarts_on_failure_then_gives_up() {
    let s = start().await;
    let sup = Supervisor::start(&s.config, s.handle.clone(), &s.dir.path().join(".kite/logs")).unwrap();
    assert_eq!(sup.len(), 1);
    let observer = Client::connect(&s.socket).await.unwrap();
    // Wait for the terminal state; the assertion is on the state, not on how long it took.
    let mut process = None;
    for _ in 0..200 {
        let st = observer.status(Some("crashy")).await.unwrap();
        process = st.detail.unwrap().process;
        if process.as_ref().is_some_and(|p| p.state == ProcessState::GaveUp) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let p = process.expect("the supervisor reported a process state");
    assert_eq!((p.state, p.restarts, p.last_exit), (ProcessState::GaveUp, 2, Some(3)));
    let log = std::fs::read_to_string(s.dir.path().join(".kite/logs/crashy.log")).unwrap();
    assert_eq!(log.matches("started crashy").count(), 3);
    sup.stop().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_senders_get_exactly_once_in_order_delivery_under_backpressure() {
    const SENDERS: usize = 8;
    const EACH: u64 = 50;
    let s = start().await;
    let sink = s.agent("sink").await;
    let mut senders = Vec::new();
    for i in 0..SENDERS {
        let c = s.agent(&format!("s{i}")).await;
        senders.push(tokio::spawn(async move {
            let mut busy = 0u32;
            for n in 0..EACH {
                let d = Draft {
                    id: None,
                    to: "sink".into(),
                    schema: "tick@1".parse().unwrap(),
                    kind: None,
                    payload: json!({ "sender": i, "n": n }),
                    timeout_ms: None,
                    blocking: false,
                };
                // The mailbox holds 64; a full one answers busy and the sender retries.
                loop {
                    match c.send(d.clone()).await {
                        Ok(_) => break,
                        Err(e) if e.code == ErrorCode::Busy => {
                            busy += 1;
                            tokio::task::yield_now().await;
                        }
                        Err(e) => panic!("send failed: {e}"),
                    }
                }
            }
            busy
        }));
    }
    let mut last = [None::<u64>; SENDERS];
    let mut ids = std::collections::HashSet::new();
    let mut lcs = Vec::new();
    while ids.len() < SENDERS * EACH as usize {
        // Each recv acknowledges the previous batch, freeing mailbox space for the senders.
        let batch = sink.recv(10_000, 32).await.unwrap();
        for m in batch.messages {
            assert!(ids.insert(m.id), "message {} delivered twice", m.id);
            let (from, n) = (m.payload["sender"].as_u64().unwrap() as usize, m.payload["n"].as_u64().unwrap());
            assert_eq!(m.from, format!("s{from}"));
            assert_eq!(last[from].map_or(0, |p| p + 1), n, "sender {from} out of order");
            assert_eq!(m.seq, n + 1);
            last[from] = Some(n);
            lcs.push(m.lc);
        }
    }
    let mut busy = 0;
    for t in senders {
        busy += t.await.unwrap();
    }
    sink.ack(Vec::new()).await.unwrap();
    assert!(lcs.windows(2).all(|w| w[0] < w[1]), "the broker clock orders all deliveries");
    assert!(last.iter().all(|l| *l == Some(EACH - 1)));
    // Not asserted: how often senders were refused, which depends on scheduling.
    eprintln!("senders were refused with busy {busy} times");
    s.shutdown().await;
}

/// Sends a status frame on a raw connection and returns the first reply line.
async fn status_reply(socket: &PathBuf) -> Value {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    stream.write_all(b"{\"id\": 1, \"op\": \"status\"}\n").await.unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap_or(Value::Null)
}

#[tokio::test]
async fn connections_over_the_limit_are_refused_until_one_closes() {
    let s = start_with(server::Limits { max_connections: 2, ..Default::default() }).await;
    let first = Client::connect(&s.socket).await.unwrap();
    let second = Client::connect(&s.socket).await.unwrap();
    first.status(None).await.unwrap();
    second.status(None).await.unwrap();
    let refused = status_reply(&s.socket).await;
    assert_eq!(refused["error"]["code"], "busy", "{refused}");

    drop(first);
    // The slot frees once the server sees the connection close.
    let mut freed = false;
    for _ in 0..200 {
        if status_reply(&s.socket).await["ok"]["protocol"] == "kite/1" {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(freed, "a closed connection gave its slot back");
    drop(second);
    s.shutdown().await;
}

#[tokio::test]
async fn a_silent_unregistered_connection_is_closed() {
    let s = start_with(server::Limits { idle_unregistered: Duration::from_millis(200), ..Default::default() }).await;
    let stream = UnixStream::connect(&s.socket).await.unwrap();
    let mut lines = BufReader::new(stream).lines();
    let goodbye = tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await.unwrap().unwrap().unwrap();
    assert!(goodbye.contains("never registered"), "{goodbye}");
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await.unwrap().unwrap(), None);
    // A registered connection may stay quiet as long as it likes.
    let quiet = Client::connect(&s.socket).await.unwrap();
    quiet.register("lead").await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    quiet.status(None).await.unwrap();
    drop(quiet);
    s.shutdown().await;
}

#[tokio::test]
async fn an_open_socket_directory_is_made_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let sock_dir = dir.path().join("run");
    std::fs::create_dir(&sock_dir).unwrap();
    std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let listener = server::bind(&sock_dir.join("kite.sock")).await.unwrap();
    let mode = std::fs::metadata(&sock_dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    drop(listener);
}
