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
- The end-to-end suites (`crates/kite-cli/tests/e2e_*.rs`) run the compiled `kite` binary
  through what users and agent hosts do: `e2e_messaging` (MCP handshake, checked request round
  trips, idempotent resend, timeouts, deadlock refusal, leases and the Claude Code hook, a
  CLI-only agent), `e2e_board` (a card from backlog to merged, real git worktrees and merges
  that must be on main, the board after a restart) and `e2e_lifecycle` (setup errors, a second
  broker refused, queued work kept across a restart, a supervised Python agent driven by
  `kite run`). Every test stops the broker with SIGTERM and requires a clean exit.
- `tests/common/mod.rs` holds the fixtures. Every wait polls for a state with a 30-second
  limit, so a hang fails with a message. A test that panics copies up.log, the ledger,
  `kite trace`, `kite board`, `kite status`, kite.toml and agent logs to
  `target/tmp/e2e-artifacts/<test>/`; CI uploads that directory when a run fails.
- CI runs the e2e suites three extra times. Locally, three suites at once is a good flake check:
  it found a startup window in which SIGTERM killed `kite up` uncleanly.
