# Architecture

## One owner for all shared state

The broker is a single tokio task that owns every mailbox, open request, lease, timer and counter.
Nothing else can touch them. Connections send it commands through a bounded channel (1024 deep)
and get replies on per-operation oneshot channels. There are no locks around broker state, so
none can be held in the wrong order. A command's position in the channel is the order in which
it takes effect, which is what gives per-sender FIFO and a total order (`lc`) across all messages.

```
connection task ──submit(op)──► [ bounded command channel ] ──► broker task
     ▲                                                          │  mailboxes, open requests,
     └────────────── oneshot reply (now, or when woken) ────────┘  leases, wait graph, timers
                                                                │
                                                     [ bounded ledger channel ]
                                                                ▼
                                                     ledger OS thread (fsync)
```

## Threads and tasks

| Unit | Kind | Why |
|---|---|---|
| broker | tokio task | sole owner of state; never blocks on I/O |
| per connection: reader | tokio task | parses frames and submits them in arrival order |
| per connection: writer | tokio task | writes replies as they complete, so a long `recv` does not hold up a `send` on the same connection |
| per pending reply | tokio task | waits on the oneshot and forwards to the writer |
| ledger writer | dedicated OS thread | file writes and fsync never stall the event loop; the bounded queue in front of it slows the broker if the disk falls behind |
| supervisor, per launched agent | tokio task | spawns, waits on and restarts one process |
| runner, per running task | tokio task in a `JoinSet` | one blocking request per task, up to `max_parallel` at once |
| MCP server, per tool call | tokio task | a host's parallel tool calls run concurrently |

## Waiting without races

`recv` and `await` with a wait park their reply channel inside the broker instead of timing out
in the connection. The broker decides whether a message is handed out or the wait expires, in one
place, so a message can never be handed to a waiter that has just given up. If the waiter's
connection has gone, sending on the oneshot fails and the message stays in the mailbox.

Timers (request deadlines, wait expiries, lease expiries) share one min-heap. Each entry carries a
generation number, so a timer for a wait that already ended, or a lease that was renewed, is
ignored when it fires.

## Deadlock refusal

A `blocking` request adds an edge `from → to` to a wait-for graph and removes it when the request
closes. Before adding an edge the broker searches for a path `to → … → from`. If one exists the
request is refused with `deadlock` and the cycle. The graph therefore never contains a cycle; a
property test checks this over random sequences of adds and removes.

## The board is event-sourced

The board belongs to the broker task like everything else, so card moves are ordered with
the messages around them. Each change is appended to the ledger as a full card snapshot. On
start the broker reads the ledger (after its chain verifies) and keeps the last snapshot of each
card. The ledger is therefore the board's only store, and every move is audited.

`board.rs` starts no processes. Moves that touch git (starting a card, merging one) call the
`Vcs` trait in `vcs.rs`, which the broker passes in on every card operation: `Git` runs the
`git` command line in the project root, and the board's unit tests pass a fake. A move runs
every other check before it asks git to change anything, so a refused move leaves the
repository as it was. Git runs on the broker task, so a slow checkout holds up other commands.

## Messages are event-sourced too

Mailboxes and open requests are rebuilt from the ledger the same way (`restore.rs`). Replaying
`accepted`, `delivered`, `acked`, `withdrawn`, `dead_lettered`, `closed` and `disconnected`
gives each mailbox and what each agent had in flight. A `started` event means every connection
of the previous run is gone, so what was in flight then goes back to the front of its mailbox
with `redelivered` incremented, as on a disconnect; the same happens for the broker starting
now. Open requests get their wall-clock `deadline_ms` back as a timer, so one that expired
while the broker was down fires on the first turn of the event loop and answers `timeout`
through the normal abort path. Blocking requests (marked `blocking` on their `accepted` event)
restore their wait-for edges. `lc` and each pair's `seq` continue from the highest values in
the ledger, so ordering stays monotonic across restarts.

What the restore decides is logged after `started`: a `restored` summary, dead letters for
messages now past `max_redeliveries`, and `peer_down` answers for requests whose sender,
recipient or schema is no longer in `kite.toml`. A later replay therefore reaches the same
state. Leases are not restored: they die with their holder's connection, and no connection
survives a restart.

## Backpressure

Every queue is bounded: the command channel, each mailbox (`mailbox_capacity`, counting messages
handed out but not acknowledged), each connection's reply queue, and the ledger queue. A full
mailbox answers `busy`. Answers to requests and the broker's own messages skip the mailbox limit,
because refusing them would leave a request with no answer.

## Testing

- Broker behaviour is tested through its public handle with a real ledger file. Deadline and
  lease tests run on tokio's paused clock, so no assertion depends on how long anything took.
- Socket tests run the real server and client, including 8 concurrent senders on a multi-threaded
  runtime checking exactly-once, in-order delivery.
- The end-to-end test runs the compiled `kite` binary: `init`, `up`, two `kite mcp` servers
  speaking JSON-RPC, the lease hook, `status`, `trace --verify`, and shutdown on SIGTERM.
