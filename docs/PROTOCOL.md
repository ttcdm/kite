# Kite protocol (`kite/1`)

Clients talk to the broker over a Unix socket, one JSON object per line in each direction. A
client may have many frames in flight; each reply carries the `id` of the frame it answers.
Frames longer than 1 MiB are refused and the connection closed.

```json
{"id": 4, "op": "send", "draft": {"to": "coder", "schema": "task.assign@1", "payload": {"goal": "x"}}}
{"id": 4, "ok": {"id": "01J…", "kind": "request", "seq": 3, "lc": 41, "deadline_ms": 1760000000000, "duplicate": false}, "inbox": 0}
{"id": 5, "error": {"code": "route_denied", "message": "coder may not send …", "detail": {"allowed": []}}, "inbox": 2}
```

`inbox` is the number of messages waiting for the connection's agent after the operation.

## Operations

| op | Needs `register` | Fields | Answer |
|---|---|---|---|
| `register` | – | `agent`, `protocol` (`"kite/1"`), `token` (required unless `require_tokens = false`) | `{agent, protocol, inbox}` |
| `send` | yes | `draft` | `{id, kind, seq, lc, deadline_ms?, duplicate}` |
| `recv` | yes | `wait_ms` (≤ 300000), `max` (1–256) | `{messages, remaining}` |
| `ack` | yes | `ids` | `{acked, unknown}` |
| `await` | yes | `request_id`, `wait_ms` | `{status: "done", response}` or `{status: "pending", request_id}` |
| `respond` | yes | `request_id`, and `result` or `error: {code, message, detail?}` | `{id, request_id}` |
| `cancel` | yes | `request_id` | `{cancelled}` |
| `lease` | yes | `resource`, `ttl_ms` | `{resource, holder, expires_at_ms}` |
| `release` | yes | `resource` | `{released}` |
| `lease_check` | no | `resource` | `{resource, lease?}` |
| `status` | no | `agent?` | every agent's summary, leases, blocking waits, and `detail` for `agent` |
| `contracts` | yes | – | routes in and out of this agent and every contract they name |
| `card` | for changes | `card: {action, …}` | the card, or `{cards}` for `list` |

`card` actions: `create {card: {title, acceptance?, merge_gate?, gates?, owner?, branch?, worktree?, id?}}`,
`update {id, patch: {…same fields…, handoff?}}`, `move {id, to, blocker?, reason?, merge_ref?}`,
`evidence {id, kind, reference, note?, gate?, pass?}`, and the read-only `get {id}` and
`list {state?, owner?}`, which work without registering.

When the project root is inside a git work tree, two moves also act on git (README.md, Agent
Kanban). `move {to: "running"}` adds the card's worktree on its branch, or creates the branch
when the card has no worktree; if git fails the answer is `transition_refused` with git's
message, and `detail.git` holds that message alone. `move {to: "merged"}` is `transition_refused`
unless `merge_ref` resolves to a commit reachable from `[board] mainline` (default `"main"`);
when it is not reachable, `detail` is `{commit, mainline}`. The merged card's `merge_ref` is the
full commit id, not the ref that was sent.

A `draft` is `{to, schema, payload, kind?, timeout_ms?, blocking?, id?}`. `kind` is taken from the
schema when omitted. `timeout_ms` and `blocking` apply to requests only. `id` is an optional
idempotency key: resending a draft with the same id returns the first send's answer with
`duplicate: true` and delivers nothing new.

## Envelopes

```json
{
  "id": "01J…", "from": "lead", "to": "coder", "kind": "request",
  "schema": "task.assign@1", "seq": 3, "lc": 41, "sent_at_ms": 1760000000000,
  "deadline_ms": 1760001800000, "redelivered": 0, "payload": {"goal": "x"}
}
```

Answers carry `correlation_id`, the id of the request they answer. Messages the broker originates
have `from: "kite"`: `kite.error@1` answers (`timeout`, `peer_down`), `kite.cancel@1` events, and
`kite.card@1` events (`{card_id, action, by, state, title}`) when a card someone else changed
concerns the recipient.

## Request lifecycle

1. `send` checks, in order: the recipient exists, the schema is declared, the kind matches, a
   route allows it, the id is not a duplicate, the payload validates, the timeout is in range,
   the recipient's mailbox has room, and (for `blocking`) no wait cycle would form. The first
   failure is the error.
2. The request waits in the recipient's mailbox until a `recv` hands it out. It is then in flight
   until the recipient acknowledges it (`ack`, its next `recv`, or `respond`).
3. It closes on the first of:
   - **respond**: the payload is checked against the request's response schema (or `kite.error@1`),
     and the answer goes to the requester even if its mailbox is full.
   - **deadline**: the requester gets `kite.error@1` with code `timeout`. The recipient loses the
     request if it never received it; otherwise it gets `kite.cancel@1` with reason `timeout`.
   - **cancel** by the requester: the same, with reason `cancelled`.
   - **peer_down**: the recipient disconnected (or the broker restarted) while holding it more
     than `max_redeliveries` times, or a restart found its sender, recipient or schema gone from
     `kite.toml`; the requester gets `peer_down`.
4. Answering a closed request fails with `no_such_request`. The detail says why it closed
   (`answered`, `timeout`, `cancelled`, `peer_down`) while the broker still remembers it.

## Disconnects

When a connection closes, the broker hands its agent's in-flight messages back to the front of the
mailbox, incrementing `redelivered`, and releases the agent's leases. Messages past
`max_redeliveries` are dead-lettered (recorded in the ledger and dropped). Blocking-wait edges
stay until the request closes, so a reconnecting agent cannot dodge deadlock detection.

## Ledger

The ledger is the directory `.kite/ledger/` (`[broker] ledger`) of segment files
`000001.jsonl`, `000002.jsonl`, …, one record per line:
`{"n", "ts_ms", "prev", "event", "hash"}`, where `hash = sha256(prev + "\n" + json({event, n, ts_ms}))`.
One chain runs through every segment: the first record of a segment continues `n` and `prev` from
the last record of the one before, and the very first `prev` is 64 zeros. A Kite 0.1 ledger file
(`.kite/ledger.jsonl`) is moved in as `000001.jsonl` on first start; an older kite would then see
an empty ledger.

When `kite up` finds the live segment larger than `[broker] segment_bytes` (default 64 MiB), it
starts the next segment with one `snapshot` record, `{format, state}`, whose `state` is the replay
state at that point (queued messages, open requests, the closed and sent windows, clocks, cards).
It is written to a temporary file, synced and renamed, so a segment always starts complete, and
it is taken just before `started`, when nothing is in flight, so the snapshot plus the later
events restore exactly what the whole history would. A start reads only the live segment and
checks its link to the segment before it; `kite trace --verify` checks every segment. Deleting
the oldest segments by hand is allowed: verification then starts at the oldest one present.
`started` and `snapshot` carry `format` (2); `kite up` refuses a ledger with a newer one.

Event types: `started`, `restored`, `stopped`, `registered`,
`disconnected`, `accepted` (`{envelope}`, plus `blocking: true` for a blocking request), `refused`, `delivered`, `acked`, `closed`, `withdrawn`, `duplicate`,
`deadlock_prevented`, `dead_lettered`, `lease_granted`, `lease_denied`, `lease_released`,
`lease_expired`, `process`, `note`, and `card` (`{action, by, card}` with the full card after the
change; the board is rebuilt from the last snapshot of each card), and `recovered`
(`{bytes_dropped}`, see below), and `snapshot` (above). `kite up` refuses to start on a live segment
whose chain does not verify.

A crash in the middle of writing a record can leave the last line incomplete. On open, a final
line without a newline is repaired before verification: if it is a complete record its newline is
restored (`bytes_dropped: 0`), otherwise it is cut off. The broker then logs `recovered` right after
`started`. Only that unterminated final line is ever touched; a complete line whose hash does not
match is tampering, and `kite up` still refuses to start.

On start the broker also rebuilds every mailbox and open request from the ledger and logs
`restored` (`{messages, requeued, open_requests, lc}`) right after `started`. Messages in flight
when the broker stopped are requeued with `redelivered` incremented, as on a disconnect, and are
dead-lettered past `max_redeliveries`. Deadlines that passed while it was down answer `timeout`
at once. A request whose sender, recipient or schema is no longer in `kite.toml` is closed and
its requester gets `peer_down`. `lc` and per-pair `seq` continue from the ledger. Leases are not
restored.

## Error codes

`invalid_frame`, `protocol_mismatch`, `not_registered`, `already_registered`, `already_connected`,
`unknown_agent`, `unauthorized`, `unknown_schema`, `route_denied`, `schema_violation`, `wrong_kind`, `busy`,
`deadlock`, `timeout`, `peer_down`, `cancelled`, `no_such_request`, `not_your_request`,
`wait_in_progress`, `lease_conflict`, `lease_not_held`, `no_such_card`, `transition_refused`,
`not_permitted`, `card_conflict`, `invalid_argument`, `shutting_down`,
`internal`. Responders may use their own codes in `respond`'s `error`.
