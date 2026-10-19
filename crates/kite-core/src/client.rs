//! Async client for the broker socket. Calls may run concurrently on one
//! client; replies are matched to calls by frame id.

use kite_proto::wire::*;
use kite_proto::{Draft, ErrorCode, KiteError, KiteResult};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use ulid::Ulid;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<ServerFrame>>>>;

pub struct Client {
    writer: tokio::sync::Mutex<OwnedWriteHalf>,
    pending: Pending,
    next: AtomicU64,
    inbox: Arc<AtomicUsize>,
    reader: JoinHandle<()>,
}

fn closed() -> KiteError {
    KiteError::new(ErrorCode::ShuttingDown, "the connection to the broker closed")
}

impl Client {
    pub async fn connect(path: &Path) -> io::Result<Client> {
        let stream = UnixStream::connect(path).await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("cannot reach the kite broker at {}: {e} (is `kite up` running?)", path.display()),
            )
        })?;
        let (r, w) = stream.into_split();
        let pending: Pending = Arc::default();
        let inbox = Arc::new(AtomicUsize::new(0));
        let reader = tokio::spawn({
            let pending = pending.clone();
            let inbox = inbox.clone();
            async move {
                let mut lines = BufReader::new(r).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let Ok(frame) = serde_json::from_str::<ServerFrame>(&line) else { continue };
                    if let Some(n) = frame.inbox {
                        inbox.store(n, Ordering::Relaxed);
                    }
                    let waiter = pending.lock().expect("pending map lock").remove(&frame.id);
                    if let Some(tx) = waiter {
                        let _ = tx.send(frame);
                    }
                }
                // Dropping the senders fails every call still waiting.
                pending.lock().expect("pending map lock").clear();
            }
        });
        Ok(Client { writer: tokio::sync::Mutex::new(w), pending, next: AtomicU64::new(1), inbox, reader })
    }

    /// Messages waiting in this agent's mailbox, as of the latest reply.
    pub fn inbox(&self) -> usize {
        self.inbox.load(Ordering::Relaxed)
    }

    pub async fn call(&self, op: Op) -> KiteResult<Value> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().expect("pending map lock").insert(id, tx);
        let mut line = serde_json::to_vec(&ClientFrame { id, op }).expect("frames serialize");
        line.push(b'\n');
        {
            let mut w = self.writer.lock().await;
            if w.write_all(&line).await.is_err() || w.flush().await.is_err() {
                self.pending.lock().expect("pending map lock").remove(&id);
                return Err(closed());
            }
        }
        let frame = rx.await.map_err(|_| closed())?;
        match (frame.ok, frame.error) {
            (_, Some(e)) => Err(e),
            (Some(v), None) => Ok(v),
            (None, None) => Ok(Value::Null),
        }
    }

    pub async fn call_as<T: DeserializeOwned>(&self, op: Op) -> KiteResult<T> {
        let v = self.call(op).await?;
        serde_json::from_value(v).map_err(|e| KiteError::new(ErrorCode::Internal, format!("unexpected reply: {e}")))
    }

    pub async fn register(&self, agent: &str) -> KiteResult<RegisterOk> {
        self.register_with(agent, None).await
    }

    /// Registers as `agent`, presenting its token.
    pub async fn register_with(&self, agent: &str, token: Option<&str>) -> KiteResult<RegisterOk> {
        let token = token.map(Secret::new);
        self.call_as(Op::Register { agent: agent.into(), protocol: PROTOCOL.into(), token }).await
    }

    pub async fn send(&self, draft: Draft) -> KiteResult<SendOk> {
        self.call_as(Op::Send { draft }).await
    }

    pub async fn recv(&self, wait_ms: u64, max: usize) -> KiteResult<RecvOk> {
        self.call_as(Op::Recv { wait_ms, max }).await
    }

    pub async fn ack(&self, ids: Vec<Ulid>) -> KiteResult<Value> {
        self.call(Op::Ack { ids }).await
    }

    pub async fn await_answer(&self, request_id: Ulid, wait_ms: u64) -> KiteResult<AwaitOk> {
        self.call_as(Op::Await { request_id, wait_ms }).await
    }

    /// Sends a blocking request and waits up to `wait` for its answer. Returns
    /// `Pending` if the wait ends first; the request stays open until its deadline.
    pub async fn request(&self, mut draft: Draft, wait: Duration) -> KiteResult<(SendOk, AwaitOk)> {
        draft.blocking = true;
        let sent = self.send(draft).await?;
        let end = tokio::time::Instant::now() + wait;
        loop {
            let left = end.saturating_duration_since(tokio::time::Instant::now()).as_millis() as u64;
            let got = self.await_answer(sent.id, left.min(MAX_WAIT_MS)).await?;
            if matches!(got, AwaitOk::Done { .. }) || left <= MAX_WAIT_MS {
                return Ok((sent, got));
            }
        }
    }

    pub async fn respond(&self, request_id: Ulid, result: Value) -> KiteResult<Value> {
        self.call(Op::Respond { request_id, result: Some(result), error: None }).await
    }

    pub async fn respond_error(&self, request_id: Ulid, error: AgentError) -> KiteResult<Value> {
        self.call(Op::Respond { request_id, result: None, error: Some(error) }).await
    }

    pub async fn cancel(&self, request_id: Ulid) -> KiteResult<Value> {
        self.call(Op::Cancel { request_id }).await
    }

    pub async fn lease(&self, resource: &str, ttl_ms: u64) -> KiteResult<LeaseInfo> {
        self.call_as(Op::Lease { resource: resource.into(), ttl_ms }).await
    }

    pub async fn release(&self, resource: &str) -> KiteResult<Value> {
        self.call(Op::Release { resource: resource.into() }).await
    }

    pub async fn lease_check(&self, resource: &str) -> KiteResult<LeaseCheckOk> {
        self.call_as(Op::LeaseCheck { resource: resource.into() }).await
    }

    pub async fn status(&self, agent: Option<&str>) -> KiteResult<StatusOk> {
        self.call_as(Op::Status { agent: agent.map(str::to_string) }).await
    }

    /// Agent Kanban operations; `get` and `list` work without registering.
    pub async fn card(&self, op: kite_proto::board::CardOp) -> KiteResult<Value> {
        self.call(Op::Card { card: op }).await
    }

    pub async fn cards(&self) -> KiteResult<Vec<kite_proto::board::Card>> {
        let v = self.card(kite_proto::board::CardOp::List { state: None, owner: None }).await?;
        serde_json::from_value(v["cards"].clone())
            .map_err(|e| KiteError::new(ErrorCode::Internal, format!("unexpected reply: {e}")))
    }

    pub async fn contracts(&self) -> KiteResult<ContractsOk> {
        self.call_as(Op::Contracts).await
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
