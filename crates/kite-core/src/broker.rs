//! The broker: one task that owns every mailbox, open request, lease and
//! timer. Connections talk to it only through a bounded command channel, so
//! there is no shared mutable state and no lock to get wrong; the order in
//! which commands reach the channel is the order in which they take effect.

use crate::board::{Applied, Board};
use crate::ledger::{Ledger, now_ms};
use crate::restore::{OpenRequest, Restored};
use crate::vcs::{Git, Vcs};
use crate::waits::WaitGraph;
use kite_proto::board::{CARD_SCHEMA, CardOp};
use kite_proto::config::BROKER_AGENT;
use kite_proto::schema::{CANCEL_SCHEMA, ERROR_SCHEMA};
use kite_proto::wire::*;
use kite_proto::*;
use serde_json::{Value, json};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep_until};
use ulid::Ulid;

pub type ConnId = u64;

/// What the broker answers one operation with.
#[derive(Debug)]
pub struct Reply {
    pub result: KiteResult<Value>,
    /// Messages waiting for the connection's agent, if it registered one.
    pub inbox: Option<usize>,
}

enum Cmd {
    Op { conn: ConnId, op: Op, reply: oneshot::Sender<Reply> },
    Disconnect { conn: ConnId },
    Note { event: Value },
    Process { agent: String, status: ProcessStatus },
    Shutdown { done: oneshot::Sender<()> },
}

const COMMAND_QUEUE: usize = 1024;
const DEDUP_WINDOW: usize = 4096;
const CLOSED_WINDOW: usize = 4096;
const MAX_RECV_BATCH: usize = 256;
const MAX_RESOURCE_LEN: usize = 1024;

/// A cheap, cloneable way to talk to the broker task.
#[derive(Clone)]
pub struct BrokerHandle {
    tx: mpsc::Sender<Cmd>,
    next_conn: Arc<AtomicU64>,
}

fn shutting_down() -> Reply {
    Reply { result: Err(KiteError::new(ErrorCode::ShuttingDown, "the broker is shutting down")), inbox: None }
}

impl Reply {
    /// The reply for an operation the broker will never answer because it stopped.
    pub fn shutting_down() -> Reply {
        shutting_down()
    }
}

impl BrokerHandle {
    /// A fresh connection id. Nothing is allocated until the connection registers.
    pub fn connect(&self) -> ConnId {
        self.next_conn.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Queues an operation and returns where its reply will arrive. Operations
    /// from one connection take effect in the order they were submitted.
    pub async fn submit(&self, conn: ConnId, op: Op) -> oneshot::Receiver<Reply> {
        let (reply, rx) = oneshot::channel();
        if let Err(mpsc::error::SendError(Cmd::Op { reply, .. })) = self.tx.send(Cmd::Op { conn, op, reply }).await {
            let _ = reply.send(shutting_down());
        }
        rx
    }

    pub async fn call(&self, conn: ConnId, op: Op) -> Reply {
        self.submit(conn, op).await.await.unwrap_or_else(|_| shutting_down())
    }

    /// The connection is gone: requeue what it held, release its leases.
    pub async fn disconnect(&self, conn: ConnId) {
        let _ = self.tx.send(Cmd::Disconnect { conn }).await;
    }

    /// Writes an event into the ledger.
    pub async fn note(&self, event: Value) {
        let _ = self.tx.send(Cmd::Note { event }).await;
    }

    /// Records the state of the OS process behind an agent (from the supervisor).
    pub async fn set_process(&self, agent: &str, status: ProcessStatus) {
        let _ = self.tx.send(Cmd::Process { agent: agent.to_string(), status }).await;
    }

    pub async fn shutdown(&self) {
        let (done, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { done }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

struct RecvWaiter {
    max: usize,
    reply: oneshot::Sender<Reply>,
    generation: u64,
}

struct AwaitWaiter {
    reply: oneshot::Sender<Reply>,
    generation: u64,
}

#[derive(Default)]
struct Slot {
    conn: Option<ConnId>,
    mailbox: VecDeque<Envelope>,
    in_flight: Vec<Envelope>,
    recv: Option<RecvWaiter>,
    awaits: HashMap<Ulid, AwaitWaiter>,
    sent: BoundedMap<SendOk>,
    process: Option<ProcessStatus>,
    last_seen_ms: Option<u64>,
}

struct Open {
    from: String,
    to: String,
    schema: SchemaRef,
    response: SchemaRef,
    deadline: Instant,
    deadline_ms: u64,
    blocking: bool,
}

struct Lease {
    holder: String,
    expires_at_ms: u64,
    generation: u64,
}

impl Lease {
    fn info(&self, resource: &str) -> LeaseInfo {
        LeaseInfo { resource: resource.to_string(), holder: self.holder.clone(), expires_at_ms: self.expires_at_ms }
    }
}

/// A map that forgets its oldest entries past a fixed size.
struct BoundedMap<V> {
    map: HashMap<Ulid, V>,
    order: VecDeque<Ulid>,
    cap: usize,
}

impl<V> BoundedMap<V> {
    fn with_cap(cap: usize) -> Self {
        BoundedMap { map: HashMap::new(), order: VecDeque::new(), cap }
    }
    fn insert(&mut self, k: Ulid, v: V) {
        if self.map.insert(k, v).is_none() {
            self.order.push_back(k);
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }
    fn get(&self, k: &Ulid) -> Option<&V> {
        self.map.get(k)
    }
}

impl<V> Default for BoundedMap<V> {
    fn default() -> Self {
        BoundedMap::with_cap(DEDUP_WINDOW)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Timer {
    Deadline(Ulid),
    Recv(String, u64),
    Await(String, Ulid, u64),
    Lease(String, u64),
}

#[derive(Debug, PartialEq, Eq)]
struct TimerEntry {
    at: Instant,
    tie: u64,
    timer: Timer,
}

impl Ord for TimerEntry {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.at, self.tie).cmp(&(o.at, o.tie))
    }
}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

pub struct Broker {
    config: Arc<Config>,
    ledger: Ledger,
    slots: BTreeMap<String, Slot>,
    conns: HashMap<ConnId, Option<String>>,
    open: HashMap<Ulid, Open>,
    closed: BoundedMap<&'static str>,
    waits: WaitGraph,
    leases: BTreeMap<String, Lease>,
    timers: BinaryHeap<Reverse<TimerEntry>>,
    tie: u64,
    generation: u64,
    lc: u64,
    seqs: HashMap<(String, String), u64>,
    log: Vec<Value>,
    board: Board,
    /// Git for the project root, used by card moves that create worktrees or check merges.
    vcs: Box<dyn Vcs + Send>,
}

fn err<T>(code: ErrorCode, message: impl Into<String>) -> KiteResult<T> {
    Err(KiteError::new(code, message))
}

fn to_value<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).expect("wire types serialize")
}

fn is_answer_to(e: &Envelope, rid: Ulid) -> bool {
    e.correlation_id == Some(rid) && matches!(e.kind, Kind::Response | Kind::Error)
}

fn op_name(op: &Op) -> &'static str {
    match op {
        Op::Register { .. } => "register",
        Op::Send { .. } => "send",
        Op::Recv { .. } => "recv",
        Op::Ack { .. } => "ack",
        Op::Await { .. } => "await",
        Op::Respond { .. } => "respond",
        Op::Cancel { .. } => "cancel",
        Op::Lease { .. } => "lease",
        Op::Release { .. } => "release",
        Op::LeaseCheck { .. } => "lease_check",
        Op::Status { .. } => "status",
        Op::Contracts => "contracts",
        Op::Card { .. } => "card",
    }
}

fn summary(e: &Envelope) -> MessageSummary {
    MessageSummary {
        id: e.id,
        from: e.from.clone(),
        kind: e.kind,
        schema: e.schema.clone(),
        lc: e.lc,
        redelivered: e.redelivered,
    }
}

/// Moves up to `max` messages from the mailbox to in-flight and sends them.
/// If the receiver is already gone, nothing moves.
fn hand_out(agent: &str, slot: &mut Slot, max: usize, reply: oneshot::Sender<Reply>) -> Option<Value> {
    let n = slot.mailbox.len().min(max);
    let messages: Vec<Envelope> = slot.mailbox.iter().take(n).cloned().collect();
    let ids: Vec<Ulid> = messages.iter().map(|e| e.id).collect();
    let remaining = slot.mailbox.len() - n;
    let r = Reply { result: Ok(to_value(&RecvOk { messages, remaining })), inbox: Some(remaining) };
    if reply.send(r).is_err() {
        return None;
    }
    slot.in_flight.extend(slot.mailbox.drain(..n));
    (!ids.is_empty()).then(|| json!({ "type": "delivered", "agent": agent, "ids": ids, "via": "recv" }))
}

impl Broker {
    /// Starts the broker task. The task ends on `shutdown`, or with an error
    /// when the ledger cannot be written: the broker stops rather than run unaudited.
    pub fn spawn(config: Arc<Config>, ledger: Ledger) -> (BrokerHandle, JoinHandle<io::Result<()>>) {
        let (tx, rx) = mpsc::channel(COMMAND_QUEUE);
        let slots = config.agents.keys().map(|a| (a.clone(), Slot::default())).collect();
        // Messages, open requests and the board are rebuilt from the ledger.
        let events = crate::ledger::read_events(ledger.path()).unwrap_or_default();
        let restored = crate::restore::replay(&events);
        let cards = crate::board::replay(events);
        let board =
            Board::new(config.board.clone(), config.agents.keys().cloned().collect(), config.root.clone(), cards);
        let vcs = Box::new(Git::new(config.root.clone()));
        let broker = Broker {
            config,
            ledger,
            slots,
            conns: HashMap::new(),
            open: HashMap::new(),
            closed: BoundedMap::with_cap(CLOSED_WINDOW),
            waits: WaitGraph::default(),
            leases: BTreeMap::new(),
            timers: BinaryHeap::new(),
            tie: 0,
            generation: 0,
            lc: 0,
            seqs: HashMap::new(),
            log: Vec::new(),
            board,
            vcs,
        };
        let task = tokio::spawn(broker.run(rx, restored));
        (BrokerHandle { tx, next_conn: Arc::new(AtomicU64::new(0)) }, task)
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Cmd>, restored: Restored) -> io::Result<()> {
        self.log(json!({
            "type": "started",
            "protocol": PROTOCOL,
            "agents": self.slots.keys().collect::<Vec<_>>(),
            "schemas": self.config.registry.contracts().map(|c| c.schema_ref.to_string()).collect::<Vec<_>>(),
        }));
        self.restore(restored);
        self.flush_log().await?;
        let mut stop: Option<oneshot::Sender<()>> = None;
        loop {
            let next = self.timers.peek().map(|Reverse(t)| t.at);
            tokio::select! {
                biased;
                cmd = rx.recv() => match cmd {
                    None => break,
                    Some(Cmd::Op { conn, op, reply }) => self.handle_op(conn, op, reply),
                    Some(Cmd::Disconnect { conn }) => self.disconnect(conn),
                    Some(Cmd::Note { event }) => self.log(json!({ "type": "note", "event": event })),
                    Some(Cmd::Process { agent, status }) => self.set_process(agent, status),
                    Some(Cmd::Shutdown { done }) => { stop = Some(done); break; }
                },
                _ = sleep_until(next.unwrap_or_else(Instant::now)), if next.is_some() => self.fire_timers(),
            }
            self.flush_log().await?;
        }
        for slot in self.slots.values_mut() {
            if let Some(w) = slot.recv.take() {
                let _ = w.reply.send(shutting_down());
            }
            for (_, w) in slot.awaits.drain() {
                let _ = w.reply.send(shutting_down());
            }
        }
        self.log(json!({ "type": "stopped", "lc": self.lc, "open_requests": self.open.len() }));
        self.flush_log().await?;
        self.ledger.flush().await?;
        let ledger = self.ledger;
        tokio::task::spawn_blocking(move || ledger.close()).await.map_err(|e| io::Error::other(e.to_string()))??;
        if let Some(done) = stop {
            let _ = done.send(());
        }
        Ok(())
    }

    fn log(&mut self, event: Value) {
        self.log.push(event);
    }

    async fn flush_log(&mut self) -> io::Result<()> {
        for e in std::mem::take(&mut self.log) {
            self.ledger.append(e).await?;
        }
        Ok(())
    }

    fn timer(&mut self, at: Instant, timer: Timer) {
        self.tie += 1;
        self.timers.push(Reverse(TimerEntry { at, tie: self.tie, timer }));
    }

    fn next_generation(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    fn inbox_of(&self, conn: ConnId) -> Option<usize> {
        let agent = self.conns.get(&conn)?.as_ref()?;
        self.slots.get(agent).map(|s| s.mailbox.len())
    }

    #[allow(clippy::too_many_arguments)]
    fn stamp(
        &mut self,
        from: &str,
        to: &str,
        kind: Kind,
        schema: SchemaRef,
        correlation_id: Option<Ulid>,
        payload: Value,
        id: Ulid,
        deadline_ms: Option<u64>,
    ) -> Envelope {
        let seq = self.seqs.entry((from.to_string(), to.to_string())).or_insert(0);
        *seq += 1;
        self.lc += 1;
        Envelope {
            id,
            from: from.to_string(),
            to: to.to_string(),
            kind,
            schema,
            correlation_id,
            seq: *seq,
            lc: self.lc,
            sent_at_ms: now_ms(),
            deadline_ms,
            redelivered: 0,
            payload,
        }
    }

    /// Puts an accepted envelope in its recipient's mailbox and wakes the recipient.
    /// `blocking` is recorded so a restart can restore the request's wait-for edge.
    fn deliver(&mut self, env: Envelope, blocking: bool) {
        let mut event = json!({ "type": "accepted", "envelope": env });
        if blocking {
            event["blocking"] = json!(true);
        }
        self.log(event);
        let to = env.to.clone();
        if let Some(slot) = self.slots.get_mut(&to) {
            slot.mailbox.push_back(env);
        }
        self.wake(&to);
    }

    fn broker_message(&mut self, to: &str, kind: Kind, schema: &str, corr: Option<Ulid>, payload: Value) {
        let schema: SchemaRef = schema.parse().expect("built-in schema refs parse");
        debug_assert!(self.config.registry.validate(&schema, &payload).is_ok());
        let env = self.stamp(BROKER_AGENT, to, kind, schema, corr, payload, Ulid::generate(), None);
        self.deliver(env, false);
    }

    /// Hands waiting agents what they are waiting for.
    fn wake(&mut self, agent: &str) {
        let mut events = Vec::new();
        let Some(slot) = self.slots.get_mut(agent) else { return };
        let wanted: Vec<Ulid> = slot.awaits.keys().copied().collect();
        for rid in wanted {
            let Some(pos) = slot.mailbox.iter().position(|e| is_answer_to(e, rid)) else { continue };
            let w = slot.awaits.remove(&rid).expect("waiter listed above");
            let env = slot.mailbox.remove(pos).expect("position found above");
            let id = env.id;
            let reply = Reply {
                result: Ok(to_value(&AwaitOk::Done { response: env.clone() })),
                inbox: Some(slot.mailbox.len()),
            };
            match w.reply.send(reply) {
                Ok(()) => events.push(json!({ "type": "delivered", "agent": agent, "ids": [id], "via": "await" })),
                Err(_) => slot.mailbox.insert(pos, env),
            }
        }
        if !slot.mailbox.is_empty()
            && let Some(w) = slot.recv.take()
        {
            events.extend(hand_out(agent, slot, w.max, w.reply));
        }
        for e in events {
            self.log(e);
        }
    }

    fn handle_op(&mut self, conn: ConnId, op: Op, reply: oneshot::Sender<Reply>) {
        self.conns.entry(conn).or_insert(None);
        let agent = self.conns.get(&conn).cloned().flatten();
        if let Some(slot) = agent.as_ref().and_then(|a| self.slots.get_mut(a)) {
            slot.last_seen_ms = Some(now_ms());
        }
        let name = op_name(&op);
        let read_only = match &op {
            Op::Status { .. } | Op::LeaseCheck { .. } | Op::Contracts => true,
            Op::Card { card } => card.is_read_only(),
            _ => false,
        };
        let mut reply = Some(reply);
        let result = match (agent.as_deref(), op) {
            (_, Op::Status { agent }) => Some(self.status(agent.as_deref())),
            (_, Op::LeaseCheck { resource }) => Some(Ok(self.lease_check(&resource))),
            (by, Op::Card { card }) if card.is_read_only() => Some(self.card_op(by.unwrap_or_default(), card)),
            (None, Op::Register { agent, protocol }) => Some(self.register(conn, &agent, &protocol)),
            (Some(_), Op::Register { .. }) => {
                Some(err(ErrorCode::AlreadyRegistered, "this connection already registered an agent"))
            }
            (None, _) => Some(err(ErrorCode::NotRegistered, "register before using this operation")),
            (Some(me), op) => {
                let me = me.to_string();
                self.agent_op(&me, op, &mut reply)
            }
        };
        // `None` means the operation took the reply: answered from a waiter or parked.
        let Some(result) = result else { return };
        if let Err(e) = &result
            && !read_only
        {
            self.log(json!({
                "type": "refused", "conn": conn, "agent": agent, "op": name,
                "code": e.code, "message": e.message,
            }));
        }
        let inbox = self.inbox_of(conn);
        if let Some(r) = reply.take() {
            let _ = r.send(Reply { result, inbox });
        }
    }

    fn agent_op(&mut self, me: &str, op: Op, reply: &mut Option<oneshot::Sender<Reply>>) -> Option<KiteResult<Value>> {
        match op {
            Op::Send { draft } => Some(self.send(me, draft)),
            Op::Recv { wait_ms, max } => self.recv(me, wait_ms, max, reply),
            Op::Ack { ids } => Some(Ok(self.ack(me, &ids))),
            Op::Await { request_id, wait_ms } => self.await_answer(me, request_id, wait_ms, reply),
            Op::Respond { request_id, result, error } => Some(self.respond(me, request_id, result, error)),
            Op::Cancel { request_id } => Some(self.cancel(me, request_id)),
            Op::Lease { resource, ttl_ms } => Some(self.lease(me, resource, ttl_ms)),
            Op::Release { resource } => Some(self.release(me, &resource)),
            Op::Contracts => Some(Ok(self.contracts(me))),
            Op::Card { card } => Some(self.card_op(me, card)),
            Op::Register { .. } | Op::Status { .. } | Op::LeaseCheck { .. } => {
                unreachable!("handled before agent_op")
            }
        }
    }

    fn register(&mut self, conn: ConnId, agent: &str, protocol: &str) -> KiteResult<Value> {
        if protocol != PROTOCOL {
            return err(
                ErrorCode::ProtocolMismatch,
                format!("the broker speaks {PROTOCOL}; the client sent {protocol:?}"),
            );
        }
        let Some(slot) = self.slots.get_mut(agent) else {
            let declared: Vec<&String> = self.slots.keys().collect();
            return Err(KiteError::new(
                ErrorCode::UnknownAgent,
                format!("{agent:?} is not declared under [agents] in kite.toml"),
            )
            .with_detail(json!({ "declared": declared })));
        };
        if slot.conn.is_some() {
            return err(ErrorCode::AlreadyConnected, format!("{agent} is already connected on another session"));
        }
        slot.conn = Some(conn);
        let inbox = slot.mailbox.len();
        self.conns.insert(conn, Some(agent.to_string()));
        self.log(json!({ "type": "registered", "agent": agent, "conn": conn }));
        Ok(to_value(&RegisterOk { agent: agent.to_string(), protocol: PROTOCOL.to_string(), inbox }))
    }

    fn send(&mut self, me: &str, d: Draft) -> KiteResult<Value> {
        let config = self.config.clone();
        if !self.slots.contains_key(&d.to) {
            let declared: Vec<&String> = self.slots.keys().collect();
            return Err(KiteError::new(ErrorCode::UnknownAgent, format!("no agent named {:?}", d.to))
                .with_detail(json!({ "declared": declared })));
        }
        let contract = config
            .registry
            .get(&d.schema)
            .ok_or_else(|| KiteError::new(ErrorCode::UnknownSchema, format!("schema {} is not declared", d.schema)))?;
        let kind = match contract.kind {
            ContractKind::Request => Kind::Request,
            ContractKind::Event => Kind::Event,
            ContractKind::Response => {
                return err(ErrorCode::WrongKind, format!("{} is a response; answer requests with respond", d.schema));
            }
        };
        if let Some(k) = d.kind
            && k != kind
        {
            return err(ErrorCode::WrongKind, format!("{} is declared as {kind:?}, not {k:?}", d.schema));
        }
        if !config.allowed(me, &d.to, &d.schema) {
            let allowed: Vec<String> = config
                .routes
                .get(&(me.to_string(), d.to.clone()))
                .map(|s| s.iter().map(|r| r.to_string()).collect())
                .unwrap_or_default();
            return Err(KiteError::new(ErrorCode::RouteDenied, format!("{me} may not send {} to {}", d.schema, d.to))
                .with_detail(json!({ "allowed": allowed })));
        }
        if let Some(id) = d.id {
            if let Some(prev) = self.slots[me].sent.get(&id) {
                let mut dup = prev.clone();
                dup.duplicate = true;
                self.log(json!({ "type": "duplicate", "agent": me, "id": id }));
                return Ok(to_value(&dup));
            }
            if self.open.contains_key(&id) {
                return err(ErrorCode::InvalidArgument, format!("message id {id} is already in use"));
            }
        }
        config.registry.validate(&d.schema, &d.payload)?;
        let b = &config.broker;
        let timeout = match kind {
            Kind::Request => {
                let t = d.timeout_ms.unwrap_or(b.default_timeout_ms);
                if t == 0 || t > b.max_timeout_ms {
                    return err(ErrorCode::InvalidArgument, format!("timeout_ms must be 1..={}", b.max_timeout_ms));
                }
                Some(t)
            }
            _ => {
                if d.timeout_ms.is_some() || d.blocking {
                    return err(ErrorCode::InvalidArgument, "only requests take timeout_ms or blocking");
                }
                None
            }
        };
        let target = &self.slots[&d.to];
        if target.mailbox.len() + target.in_flight.len() >= b.mailbox_capacity {
            return Err(KiteError::new(ErrorCode::Busy, format!("{}'s mailbox is full; retry later", d.to))
                .with_detail(json!({ "capacity": b.mailbox_capacity })));
        }
        if d.blocking
            && let Err(cycle) = self.waits.try_add(me, &d.to)
        {
            self.log(json!({ "type": "deadlock_prevented", "agent": me, "to": d.to, "cycle": cycle }));
            return Err(KiteError::new(
                ErrorCode::Deadlock,
                format!("waiting on {} would deadlock: {}", d.to, cycle.join(" -> ")),
            )
            .with_detail(json!({ "cycle": cycle })));
        }
        let id = d.id.unwrap_or_else(Ulid::generate);
        let deadline_ms = timeout.map(|t| now_ms() + t);
        let env = self.stamp(me, &d.to, kind, d.schema.clone(), None, d.payload, id, deadline_ms);
        if let (Some(t), Some(response)) = (timeout, contract.response.clone()) {
            let deadline = Instant::now() + Duration::from_millis(t);
            self.open.insert(
                id,
                Open {
                    from: me.to_string(),
                    to: d.to.clone(),
                    schema: d.schema.clone(),
                    response,
                    deadline,
                    deadline_ms: deadline_ms.unwrap_or_default(),
                    blocking: d.blocking,
                },
            );
            self.timer(deadline, Timer::Deadline(id));
        }
        let ok = SendOk { id, kind, seq: env.seq, lc: env.lc, deadline_ms, duplicate: false };
        if let Some(slot) = self.slots.get_mut(me) {
            slot.sent.insert(id, ok.clone());
        }
        self.deliver(env, d.blocking);
        Ok(to_value(&ok))
    }

    fn recv(
        &mut self,
        me: &str,
        wait_ms: u64,
        max: usize,
        reply: &mut Option<oneshot::Sender<Reply>>,
    ) -> Option<KiteResult<Value>> {
        if wait_ms > MAX_WAIT_MS || max == 0 || max > MAX_RECV_BATCH {
            return Some(err(
                ErrorCode::InvalidArgument,
                format!("wait_ms must be 0..={MAX_WAIT_MS} and max 1..={MAX_RECV_BATCH}"),
            ));
        }
        let generation = self.next_generation();
        let slot = self.slots.get_mut(me).expect("registered agents have slots");
        if slot.recv.is_some() {
            return Some(err(ErrorCode::WaitInProgress, "a recv is already waiting for this agent"));
        }
        // Asking for more acknowledges everything handed out before.
        let acked: Vec<Ulid> = slot.in_flight.drain(..).map(|e| e.id).collect();
        let mut events = Vec::new();
        if !acked.is_empty() {
            events.push(json!({ "type": "acked", "agent": me, "ids": acked }));
        }
        let r = reply.take().expect("reply present until taken");
        if !slot.mailbox.is_empty() || wait_ms == 0 {
            events.extend(hand_out(me, slot, max, r));
        } else {
            slot.recv = Some(RecvWaiter { max, reply: r, generation });
            self.timer(Instant::now() + Duration::from_millis(wait_ms), Timer::Recv(me.to_string(), generation));
        }
        for e in events {
            self.log(e);
        }
        None
    }

    fn ack(&mut self, me: &str, ids: &[Ulid]) -> Value {
        let slot = self.slots.get_mut(me).expect("registered agents have slots");
        let held: BTreeSet<Ulid> = slot.in_flight.iter().map(|e| e.id).collect();
        slot.in_flight.retain(|e| !ids.contains(&e.id));
        let (acked, unknown): (Vec<Ulid>, Vec<Ulid>) = ids.iter().partition(|id| held.contains(id));
        if !acked.is_empty() {
            self.log(json!({ "type": "acked", "agent": me, "ids": acked }));
        }
        json!({ "acked": acked.len(), "unknown": unknown })
    }

    fn no_such_request(&self, rid: Ulid) -> KiteError {
        match self.closed.get(&rid) {
            Some(reason) => KiteError::new(ErrorCode::NoSuchRequest, format!("request {rid} is closed ({reason})"))
                .with_detail(json!({ "closed": reason })),
            None => KiteError::new(ErrorCode::NoSuchRequest, format!("no open request {rid}")),
        }
    }

    fn await_answer(
        &mut self,
        me: &str,
        rid: Ulid,
        wait_ms: u64,
        reply: &mut Option<oneshot::Sender<Reply>>,
    ) -> Option<KiteResult<Value>> {
        if wait_ms > MAX_WAIT_MS {
            return Some(err(ErrorCode::InvalidArgument, format!("wait_ms must be 0..={MAX_WAIT_MS}")));
        }
        let generation = self.next_generation();
        let slot = self.slots.get_mut(me).expect("registered agents have slots");
        let found = match slot.mailbox.iter().position(|e| is_answer_to(e, rid)) {
            Some(pos) => slot.mailbox.remove(pos),
            None => slot.in_flight.iter().position(|e| is_answer_to(e, rid)).map(|pos| slot.in_flight.remove(pos)),
        };
        if let Some(env) = found {
            let id = env.id;
            self.log(json!({ "type": "delivered", "agent": me, "ids": [id], "via": "await" }));
            return Some(Ok(to_value(&AwaitOk::Done { response: env })));
        }
        match self.open.get(&rid) {
            Some(o) if o.from != me => {
                Some(err(ErrorCode::NotYourRequest, format!("request {rid} was sent by {}", o.from)))
            }
            Some(_) if wait_ms == 0 => Some(Ok(to_value(&AwaitOk::Pending { request_id: rid }))),
            Some(_) => {
                if slot.awaits.contains_key(&rid) {
                    return Some(err(ErrorCode::WaitInProgress, format!("an await is already waiting on {rid}")));
                }
                let r = reply.take().expect("reply present until taken");
                slot.awaits.insert(rid, AwaitWaiter { reply: r, generation });
                self.timer(
                    Instant::now() + Duration::from_millis(wait_ms),
                    Timer::Await(me.to_string(), rid, generation),
                );
                None
            }
            None => Some(Err(self.no_such_request(rid))),
        }
    }

    fn respond(&mut self, me: &str, rid: Ulid, result: Option<Value>, error: Option<AgentError>) -> KiteResult<Value> {
        let config = self.config.clone();
        let (kind, payload) = match (result, error) {
            (Some(r), None) => (Kind::Response, r),
            (None, Some(e)) => (Kind::Error, to_value(&e)),
            _ => return err(ErrorCode::InvalidArgument, "give exactly one of result or error"),
        };
        let Some(o) = self.open.get(&rid) else { return Err(self.no_such_request(rid)) };
        if o.to != me {
            return err(ErrorCode::NotYourRequest, format!("request {rid} is addressed to {}", o.to));
        }
        let schema = match kind {
            Kind::Response => o.response.clone(),
            _ => ERROR_SCHEMA.parse().expect("built-in schema refs parse"),
        };
        config.registry.validate(&schema, &payload)?;
        let o = self.close(rid, "answered").expect("checked open above");
        if let Some(slot) = self.slots.get_mut(me) {
            slot.in_flight.retain(|e| e.id != rid);
            slot.mailbox.retain(|e| e.id != rid);
        }
        let env = self.stamp(me, &o.from, kind, schema, Some(rid), payload, Ulid::generate(), None);
        let id = env.id;
        self.deliver(env, false);
        Ok(json!({ "id": id, "request_id": rid }))
    }

    fn close(&mut self, rid: Ulid, reason: &'static str) -> Option<Open> {
        let o = self.open.remove(&rid)?;
        if o.blocking {
            self.waits.remove(&o.from, &o.to);
        }
        self.closed.insert(rid, reason);
        self.log(json!({ "type": "closed", "request_id": rid, "reason": reason }));
        Some(o)
    }

    /// Ends an open request without an answer from its recipient. The recipient
    /// loses the request if it has not picked it up, or gets `kite.cancel@1`.
    fn abort(&mut self, rid: Ulid, code: ErrorCode, reason: &'static str, tell_requester: bool) {
        let Some(o) = self.close(rid, reason) else { return };
        let mut withdrawn = false;
        if let Some(slot) = self.slots.get_mut(&o.to)
            && let Some(pos) = slot.mailbox.iter().position(|e| e.id == rid)
        {
            slot.mailbox.remove(pos);
            withdrawn = true;
        }
        if withdrawn {
            self.log(json!({ "type": "withdrawn", "agent": o.to, "id": rid }));
        } else {
            self.broker_message(
                &o.to,
                Kind::Event,
                CANCEL_SCHEMA,
                None,
                json!({ "request_id": rid.to_string(), "reason": reason }),
            );
        }
        if tell_requester {
            let message = match code {
                ErrorCode::Timeout => format!("{} did not answer {} before its deadline", o.to, o.schema),
                _ => format!("request {rid} to {} ended: {reason}", o.to),
            };
            self.broker_message(
                &o.from,
                Kind::Error,
                ERROR_SCHEMA,
                Some(rid),
                json!({ "code": code.as_str(), "message": message }),
            );
        }
    }

    fn cancel(&mut self, me: &str, rid: Ulid) -> KiteResult<Value> {
        match self.open.get(&rid) {
            None => return Err(self.no_such_request(rid)),
            Some(o) if o.from != me => {
                return err(ErrorCode::NotYourRequest, format!("request {rid} was sent by {}", o.from));
            }
            Some(_) => {}
        }
        self.abort(rid, ErrorCode::Cancelled, "cancelled", false);
        if let Some(w) = self.slots.get_mut(me).and_then(|s| s.awaits.remove(&rid)) {
            let _ = w
                .reply
                .send(Reply { result: err(ErrorCode::Cancelled, format!("request {rid} was cancelled")), inbox: None });
        }
        Ok(json!({ "cancelled": rid }))
    }

    fn lease(&mut self, me: &str, resource: String, ttl_ms: u64) -> KiteResult<Value> {
        let resource = resource.trim().to_string();
        if resource.is_empty() || resource.len() > MAX_RESOURCE_LEN {
            return err(ErrorCode::InvalidArgument, format!("resource must be 1..={MAX_RESOURCE_LEN} bytes"));
        }
        let max = self.config.broker.max_lease_ms;
        if ttl_ms == 0 || ttl_ms > max {
            return err(ErrorCode::InvalidArgument, format!("ttl_ms must be 1..={max}"));
        }
        if let Some(l) = self.leases.get(&resource)
            && l.holder != me
        {
            let info = l.info(&resource);
            self.log(json!({ "type": "lease_denied", "agent": me, "resource": resource, "holder": l.holder }));
            return Err(KiteError::new(ErrorCode::LeaseConflict, format!("{resource} is leased by {}", info.holder))
                .with_detail(to_value(&info)));
        }
        let generation = self.next_generation();
        let renewed = self.leases.contains_key(&resource);
        let expires_at_ms = now_ms() + ttl_ms;
        self.leases.insert(resource.clone(), Lease { holder: me.to_string(), expires_at_ms, generation });
        self.timer(Instant::now() + Duration::from_millis(ttl_ms), Timer::Lease(resource.clone(), generation));
        self.log(
            json!({ "type": "lease_granted", "agent": me, "resource": resource, "ttl_ms": ttl_ms, "renewed": renewed }),
        );
        Ok(to_value(&LeaseInfo { resource, holder: me.to_string(), expires_at_ms }))
    }

    fn release(&mut self, me: &str, resource: &str) -> KiteResult<Value> {
        let resource = resource.trim();
        match self.leases.get(resource) {
            Some(l) if l.holder == me => {
                self.leases.remove(resource);
                self.log(json!({ "type": "lease_released", "agent": me, "resource": resource }));
                Ok(json!({ "released": resource }))
            }
            Some(l) => err(ErrorCode::LeaseNotHeld, format!("{resource} is held by {}, not {me}", l.holder)),
            None => err(ErrorCode::LeaseNotHeld, format!("{resource} is not leased")),
        }
    }

    fn lease_check(&self, resource: &str) -> Value {
        let resource = resource.trim();
        let lease = self.leases.get(resource).map(|l| l.info(resource));
        to_value(&LeaseCheckOk { resource: resource.to_string(), lease })
    }

    fn leases_of(&self, agent: &str) -> Vec<LeaseInfo> {
        self.leases.iter().filter(|(_, l)| l.holder == agent).map(|(r, l)| l.info(r)).collect()
    }

    fn requests(&self, pred: impl Fn(&Open) -> bool) -> Vec<RequestSummary> {
        let mut v: Vec<RequestSummary> = self
            .open
            .iter()
            .filter(|(_, o)| pred(o))
            .map(|(id, o)| RequestSummary {
                id: *id,
                from: o.from.clone(),
                to: o.to.clone(),
                schema: o.schema.clone(),
                deadline_ms: o.deadline_ms,
                blocking: o.blocking,
            })
            .collect();
        v.sort_by_key(|r| r.id);
        v
    }

    fn status(&self, agent: Option<&str>) -> KiteResult<Value> {
        let agents = self
            .slots
            .iter()
            .map(|(name, s)| AgentStatus {
                name: name.clone(),
                connected: s.conn.is_some(),
                mailbox: s.mailbox.len(),
                in_flight: s.in_flight.len(),
                awaiting: self.open.values().filter(|o| &o.from == name).count(),
                owes: self.open.values().filter(|o| &o.to == name).count(),
                leases: self.leases.values().filter(|l| &l.holder == name).count(),
                cards: self.board.active_owned_by(name).count(),
                last_seen_ms: s.last_seen_ms,
                process: s.process.clone(),
            })
            .collect();
        let detail = match agent {
            None => None,
            Some(a) => {
                let s = self.slots.get(a).ok_or_else(|| {
                    KiteError::new(ErrorCode::UnknownAgent, format!("{a:?} is not declared under [agents]"))
                })?;
                let mut in_await: Vec<Ulid> = s.awaits.keys().copied().collect();
                in_await.sort();
                Some(AgentDetail {
                    name: a.to_string(),
                    description: self.config.agents.get(a).and_then(|x| x.description.clone()),
                    connected: s.conn.is_some(),
                    in_recv: s.recv.is_some(),
                    in_await,
                    mailbox: s.mailbox.iter().map(summary).collect(),
                    in_flight: s.in_flight.iter().map(summary).collect(),
                    awaiting: self.requests(|o| o.from == a),
                    owes: self.requests(|o| o.to == a),
                    leases: self.leases_of(a),
                    cards: self.board.active_owned_by(a).map(|c| c.id.clone()).collect(),
                    last_seen_ms: s.last_seen_ms,
                    process: s.process.clone(),
                })
            }
        };
        let leases = self.leases.iter().map(|(r, l)| l.info(r)).collect();
        Ok(to_value(&StatusOk {
            protocol: PROTOCOL.to_string(),
            lc: self.lc,
            agents,
            leases,
            waits: self.waits.pairs(),
            detail,
        }))
    }

    fn contracts(&self, me: &str) -> Value {
        let reg = &self.config.registry;
        let mut named: BTreeSet<SchemaRef> = [ERROR_SCHEMA, CANCEL_SCHEMA, CARD_SCHEMA]
            .iter()
            .map(|s| s.parse().expect("built-in schema refs parse"))
            .collect();
        let mut may_send = Vec::new();
        let mut may_receive = Vec::new();
        let mut keys: Vec<&(String, String)> = self.config.routes.keys().collect();
        keys.sort();
        for key in keys {
            let schemas = &self.config.routes[key];
            let (from, to) = key;
            if from == me {
                may_send.push(RouteView { peer: to.clone(), schemas: schemas.iter().cloned().collect() });
            } else if to == me {
                may_receive.push(RouteView { peer: from.clone(), schemas: schemas.iter().cloned().collect() });
            } else {
                continue;
            }
            for s in schemas {
                named.insert(s.clone());
                if let Some(r) = reg.get(s).and_then(|c| c.response.clone()) {
                    named.insert(r);
                }
            }
        }
        let contracts = named
            .iter()
            .filter_map(|r| reg.get(r))
            .map(|c| ContractView {
                schema: c.schema_ref.clone(),
                kind: c.kind,
                response: c.response.clone(),
                response_schema: c.response.as_ref().and_then(|r| reg.get(r)).map(|rc| rc.json_schema.clone()),
                description: c.description.clone(),
                json_schema: c.json_schema.clone(),
            })
            .collect();
        to_value(&ContractsOk {
            agent: me.to_string(),
            description: self.config.agents.get(me).and_then(|a| a.description.clone()),
            may_send,
            may_receive,
            contracts,
        })
    }

    /// Applies a card operation; a change is recorded as a full card snapshot
    /// (which is what the board is rebuilt from) and announced to the agents it concerns.
    fn card_op(&mut self, by: &str, op: CardOp) -> KiteResult<Value> {
        match self.board.apply(by, op, now_ms(), self.vcs.as_ref())? {
            Applied::Read(v) => Ok(v),
            Applied::Changed { card, action, notify } => {
                self.log(json!({ "type": "card", "action": action, "by": by, "card": card }));
                for agent in notify {
                    let payload = json!({
                        "card_id": card.id, "action": action, "by": by,
                        "state": card.state.as_str(), "title": card.title,
                    });
                    self.broker_message(&agent, Kind::Event, CARD_SCHEMA, None, payload);
                }
                Ok(to_value(&*card))
            }
        }
    }

    fn set_process(&mut self, agent: String, status: ProcessStatus) {
        self.log(json!({ "type": "process", "agent": agent, "status": status }));
        if let Some(slot) = self.slots.get_mut(&agent) {
            slot.process = Some(status);
        }
    }

    fn disconnect(&mut self, conn: ConnId) {
        let Some(Some(agent)) = self.conns.remove(&conn) else { return };
        let max = self.config.broker.max_redeliveries;
        let Some(slot) = self.slots.get_mut(&agent) else { return };
        if slot.conn != Some(conn) {
            return;
        }
        slot.conn = None;
        slot.recv = None;
        slot.awaits.clear();
        let mut requeued = Vec::new();
        let mut dead = Vec::new();
        for mut env in std::mem::take(&mut slot.in_flight).into_iter().rev() {
            env.redelivered += 1;
            if env.redelivered > max {
                dead.push(env);
            } else {
                requeued.push(env.id);
                slot.mailbox.push_front(env);
            }
        }
        let held: Vec<String> = self.leases.iter().filter(|(_, l)| l.holder == agent).map(|(r, _)| r.clone()).collect();
        for r in &held {
            self.leases.remove(r);
        }
        self.log(json!({
            "type": "disconnected", "agent": agent, "conn": conn,
            "requeued": requeued, "leases_released": held,
        }));
        for env in dead {
            self.dead_letter(&agent, env);
        }
    }

    /// Drops a message redelivered past `max_redeliveries`; a request ends with `peer_down`.
    fn dead_letter(&mut self, agent: &str, env: Envelope) {
        self.log(json!({ "type": "dead_lettered", "agent": agent, "id": env.id, "redelivered": env.redelivered }));
        if env.kind == Kind::Request
            && let Some(o) = self.close(env.id, "peer_down")
        {
            self.broker_message(
                &o.from,
                Kind::Error,
                ERROR_SCHEMA,
                Some(env.id),
                json!({
                    "code": ErrorCode::PeerDown.as_str(),
                    "message": format!("{agent} disconnected {} times while holding this request", env.redelivered),
                }),
            );
        }
    }

    /// Puts back what the ledger says was queued and open when the broker last
    /// stopped. It runs right after `started` is logged, so what it decides
    /// (dead letters, requests that can no longer be served) follows that event
    /// and a later replay sees the same thing. Deadlines that passed while the
    /// broker was down fire on the first turn of the event loop.
    fn restore(&mut self, r: Restored) {
        self.lc = r.lc;
        self.seqs = r.seqs;
        for (rid, reason) in r.closed {
            if let Some(reason) = ["answered", "timeout", "cancelled", "peer_down"].into_iter().find(|k| *k == reason) {
                self.closed.insert(rid, reason);
            }
        }
        for (agent, ok) in r.sent {
            if let Some(slot) = self.slots.get_mut(&agent) {
                slot.sent.insert(ok.id, ok);
            }
        }
        let max = self.config.broker.max_redeliveries;
        let mut dead = Vec::new();
        let mut messages = 0;
        for (agent, envs) in r.mailboxes {
            // Messages for an agent no longer in kite.toml have nowhere to go.
            let Some(slot) = self.slots.get_mut(&agent) else { continue };
            for env in envs {
                if env.redelivered > max {
                    dead.push((agent.clone(), env));
                } else {
                    messages += 1;
                    slot.mailbox.push_back(env);
                }
            }
        }
        let (now, wall) = (Instant::now(), now_ms());
        let mut orphans = Vec::new();
        for o in r.open {
            let response = self.config.registry.get(&o.schema).and_then(|c| c.response.clone());
            let declared = self.slots.contains_key(&o.from) && self.slots.contains_key(&o.to);
            let Some(response) = response.filter(|_| declared) else {
                orphans.push(o);
                continue;
            };
            let deadline = now + Duration::from_millis(o.deadline_ms.saturating_sub(wall));
            // The edges come from a state that had no cycle, so they all fit again.
            if o.blocking {
                let _ = self.waits.try_add(&o.from, &o.to);
            }
            let open = Open {
                from: o.from,
                to: o.to,
                schema: o.schema,
                response,
                deadline,
                deadline_ms: o.deadline_ms,
                blocking: o.blocking,
            };
            self.open.insert(o.id, open);
            self.timer(deadline, Timer::Deadline(o.id));
        }
        self.log(json!({
            "type": "restored", "messages": messages, "requeued": r.requeued,
            "open_requests": self.open.len(), "lc": self.lc,
        }));
        for (agent, env) in dead {
            self.dead_letter(&agent, env);
        }
        for o in orphans {
            self.orphan(o);
        }
    }

    /// Ends a restored request that kite.toml no longer allows to be answered:
    /// its sender, recipient or schema is gone. The requester gets `peer_down`.
    fn orphan(&mut self, o: OpenRequest) {
        self.closed.insert(o.id, "peer_down");
        self.log(json!({ "type": "closed", "request_id": o.id, "reason": "peer_down" }));
        if let Some(slot) = self.slots.get_mut(&o.to)
            && let Some(pos) = slot.mailbox.iter().position(|e| e.id == o.id)
        {
            slot.mailbox.remove(pos);
            self.log(json!({ "type": "withdrawn", "agent": o.to, "id": o.id }));
        }
        if self.slots.contains_key(&o.from) {
            let message = format!("{} to {} can no longer be answered: kite.toml changed", o.schema, o.to);
            let payload = json!({ "code": ErrorCode::PeerDown.as_str(), "message": message });
            self.broker_message(&o.from, Kind::Error, ERROR_SCHEMA, Some(o.id), payload);
        }
    }

    fn fire_timers(&mut self) {
        let now = Instant::now();
        while self.timers.peek().is_some_and(|Reverse(t)| t.at <= now) {
            let Reverse(entry) = self.timers.pop().expect("peeked above");
            match entry.timer {
                Timer::Deadline(rid) => {
                    if self.open.get(&rid).is_some_and(|o| o.deadline <= now) {
                        self.abort(rid, ErrorCode::Timeout, "timeout", true);
                    }
                }
                Timer::Recv(agent, generation) => {
                    let Some(slot) = self.slots.get_mut(&agent) else { continue };
                    if slot.recv.as_ref().is_some_and(|w| w.generation == generation) {
                        let w = slot.recv.take().expect("checked above");
                        if let Some(e) = hand_out(&agent, slot, w.max, w.reply) {
                            self.log(e);
                        }
                    }
                }
                Timer::Await(agent, rid, generation) => {
                    let Some(slot) = self.slots.get_mut(&agent) else { continue };
                    if slot.awaits.get(&rid).is_some_and(|w| w.generation == generation) {
                        let w = slot.awaits.remove(&rid).expect("checked above");
                        let inbox = Some(slot.mailbox.len());
                        let _ =
                            w.reply.send(Reply { result: Ok(to_value(&AwaitOk::Pending { request_id: rid })), inbox });
                    }
                }
                Timer::Lease(resource, generation) => {
                    if self.leases.get(&resource).is_some_and(|l| l.generation == generation) {
                        let l = self.leases.remove(&resource).expect("checked above");
                        self.log(json!({ "type": "lease_expired", "agent": l.holder, "resource": resource }));
                    }
                }
            }
        }
    }
}
