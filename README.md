# Kite

Kite runs a team of coding agents (Claude Code, Codex CLI, Gemini CLI, Cursor, Aider, or any
process that can run a command) and makes every message between them go through one broker that
checks it. An agent cannot send a message to someone it has no route to, cannot send a payload that
breaks the contract, and cannot leave a request unanswered: the broker answers with `timeout` at the
deadline.

```
 Claude Code    Codex CLI    Gemini CLI    shell agent      kite run (task graph)
     │ MCP          │ MCP        │ MCP          │ CLI              │ client
 kite mcp       kite mcp     kite mcp         kite …              │
     └──────────────┴────────────┴──── Unix socket, JSON lines ───┘
                                 broker (kite up)
      routes · JSON Schema contracts · bounded mailboxes · deadlines · deadlock refusal
      leases · supervisor · hash-chained ledger
```

## Quick start

```bash
cargo install --path crates/kite-cli
```

```bash
kite init
```

```bash
kite up
```

`kite init` writes a starter team (`lead`, `coder`, `reviewer`) to `kite.toml`, its schemas to
`schemas/`, and one MCP config per agent to `.kite/mcp/<agent>.json`. It also prints how to plug
each host in. For Claude Code, for example:

```bash
claude mcp add kite -- kite mcp --agent coder
```

`kite connect <agent>` prints the snippets for one agent: Claude Code, hosts with a JSON
`mcpServers` file (Claude Code `.mcp.json`, Cursor, Gemini CLI, Windsurf, Cline), and Codex CLI's
`config.toml`. These follow each host's documented format; check them against the version you run.

## What "strict" means here

| Rule | Enforced by |
|---|---|
| Only agents declared in `kite.toml` can connect, one live connection each | `register` |
| A message needs a declared route `from → to` for its schema | router (`route_denied`, with the allowed schemas) |
| Payloads must match the schema's JSON Schema | validator (`schema_violation`, with each failing path) |
| A schema's kind (request, response, event) is fixed | `wrong_kind` |
| Every request gets exactly one answer: the recipient's response or error, `timeout` at the deadline, `cancelled`, or `peer_down` | open-request table and deadline timers |
| A response must match the request's declared response schema and come from the request's recipient | `respond` |
| A blocking request that would close a wait cycle is refused | wait-for graph (`deadlock`, with the cycle) |
| A full mailbox refuses new requests and events; answers still get through | `busy` |
| Messages from one sender to one recipient arrive in order, numbered by `seq`; the broker clock `lc` orders everything | single broker task |
| Resending a message id delivers it once | per-sender dedup window |
| A message handed out but not acknowledged is redelivered if its agent disconnects, up to `max_redeliveries` | in-flight list |
| Exclusive leases on files or any named resource; they end at their TTL or when the holder disconnects | lease table |
| Everything accepted, refused and decided is written to a hash-chained ledger; the broker stops if it cannot write it | `.kite/ledger.jsonl` |

## Agent Kanban

Work items live on a board the broker owns, next to the messages. A card has an owner, a
branch or worktree, acceptance criteria, a merge gate in words, and an ordered list of named
gates. The broker refuses any move whose exit criteria are not met and says which one is missing:

| Move | Exit criteria |
|---|---|
| backlog → ready | acceptance criteria, a merge gate and at least one gate; by a manager |
| ready → running | an owner and a branch or worktree; only the owner starts it; no other active card may use the same worktree. In a git project the broker then adds the worktree on the card's branch (`git worktree add`, creating the branch from HEAD if it does not exist, or checking it out in a worktree already there), or creates the branch if the card has no worktree; a worktree needs a branch, and a git error refuses the move with git's message |
| running → review | the handoff file (path relative to the worktree) exists and evidence was recorded for this revision; by the owner |
| review → merged | every gate passed in order in this revision, at least one passed by someone other than the owner (`independent_review`), and a `merge_ref`; in a git project `merge_ref` must name a commit reachable from `mainline`, and the card keeps the full commit id; by a manager |
| review → running | starts a new revision; earlier gate results no longer count |
| any → blocked | a blocker with reason, owner and next action; it can only return to the column it left |
| any → archived | a reason; by a manager |

```toml
[board]
managers = ["lead"]                        # empty: every agent may manage
default_gates = ["tests", "diff_review", "risk"]
independent_review = true
mainline = "main"                          # merge_ref must be on this ref (git projects only)
```

Card changes are written to the ledger as full snapshots, and `kite up` rebuilds the board
from them, so the board survives restarts. When someone else changes a card, its owner gets a
`kite.card@1` event; managers get one when a card starts, is handed off, is blocked or fails a
gate.

```bash
kite board
```

`kite board` is the control pane. It shows each column with owner, last heartbeat and
worktree, blockers grouped by owner with their next action, and merge readiness by gate for
every card in review. `--all` lists merged and archived cards; `--watch` redraws every second.
`kite card new|show|set|move|evidence` changes cards from a shell; agents use `kite_board`,
`kite_card_create`, `kite_card_update`, `kite_card_move` and `kite_card_evidence`.

A git project is one whose root (the directory holding `kite.toml`) is inside a git work tree.
There the broker creates branches and worktrees when cards start and checks `merge_ref` against
`mainline` when they merge. In any other project it checks only that the handoff file exists, and
`merge_ref` is kept as written (a PR link is fine). The broker does not check that the commit
in `merge_ref` contains the card's branch, does not remove worktrees when a card merges or is
archived, and does not re-check the branch when a card is reopened or unblocked. Git runs on
the broker task, so a slow checkout holds up other messages until it finishes.

## Seeing what each agent is doing

```bash
kite status
```

```bash
kite status coder
```

```bash
kite status --watch
```

`kite status` prints one row per agent: connected or not, when it was last seen (its last
operation), messages queued, messages handed out but not acknowledged, open requests it sent and
open requests it owes, leases, active cards, and the process state if `kite up` launched it. `kite status <agent>` lists that agent's queued messages, open
requests in both directions with deadlines, its leases, whether it is waiting in a recv, and its
process. Agents can see the same through the `kite_status` tool.

`kite trace` prints the ledger (`--agent coder` for one agent), and `kite trace --verify` checks
its hash chain.

## Agent tools (MCP)

`kite mcp --agent <name>` serves these tools over stdio:

| Tool | Does |
|---|---|
| `kite_contracts` | Who you may message, with which schemas, and each JSON Schema. Agents should call it first. |
| `kite_send` | Send an event, or a request whose answer you collect later |
| `kite_request` | Send a request and wait up to `wait_ms` (at most 25 s) for its answer |
| `kite_await` | Wait for the answer to a request you sent |
| `kite_recv` | Get messages; calling it again acknowledges the previous batch |
| `kite_respond` | Answer a request with a result or an error |
| `kite_cancel` | Withdraw a request you sent |
| `kite_lease` / `kite_release` | Exclusive leases; project paths are normalised to root-relative keys |
| `kite_status` | Every agent's state, or one agent in detail |
| `kite_board`, `kite_card_*` | Read the board; create, update, move cards and attach evidence or gate results |

Every tool result includes `inbox`, the number of messages waiting, because MCP has no way for a
server to wake an agent. An agent learns of new mail on its next tool call or `kite_recv`.

## Shell agents

Anything that can run a command can take part:

```bash
kite recv --as coder --wait 20
```

```bash
kite respond --as coder --request 01J... --result '{"summary":"done","files_changed":[]}'
```

```bash
kite send --as lead --to coder --schema task.assign@1 --payload '{"goal":"add a flag"}' --request
```

`kite send --request` exits 0 when answered, 1 on an error answer, and 3 if still pending.
`kite recv` acknowledges what it printed unless `--keep` is given.

## Task graphs

`kite run plan.toml` registers as the plan's `as` agent and sends one blocking request per task.
Tasks whose dependencies are done run concurrently, up to `max_parallel`. `${tasks.<id>.result}`
in a payload is replaced by that task's result. A failed task skips its dependents, and
`fail_fast = true` cancels everything still running. See [examples/plan.toml](examples/plan.toml).

## Launching agents

An agent with a `command` in `kite.toml` is launched by `kite up` and restarted per `restart`
(`never`, `on-failure`, `always`) up to `max_restarts`, with exponential backoff. `{prompt}`,
`{agent}`, `{socket}` and `{root}` in the command are filled in. The process also gets
`KITE_AGENT`, `KITE_SOCKET` and `KITE_ROOT` in its environment, and its output goes to
`.kite/logs/<agent>.log`. Each agent restarts on its own (one_for_one); there is no group restart.

Each launched agent leads its own process group, so stopping `kite up` also stops whatever the
agent started (an MCP server, a shell, a test run): SIGTERM to the group, then SIGKILL after
`stop_grace_ms` (default 5000). The group id is kept in `.kite/run/<agent>.pid`. If `kite up` is
killed with SIGKILL, nothing can stop its agents; the next `kite up` warns about any that are still
running (in its log and as a `stale_agent_group` ledger note) but does not kill them.

## Claude Code lease guard

```json
{"hooks": {"PreToolUse": [{"matcher": "Edit|Write|MultiEdit|NotebookEdit",
  "hooks": [{"type": "command", "command": "kite hook claude-code --agent coder"}]}]}}
```

The hook blocks an edit (exit code 2) when another agent holds the file's lease. With `--strict`
it also blocks edits to files the agent has not leased. If the broker is unreachable it allows the
edit and prints a warning. This hook is Claude Code specific; other hosts rely on agents calling
`kite_lease`.

## Limits

- One machine, one broker, Unix sockets only (macOS and Linux). Socket paths are limited to about
  104 bytes on macOS; set `broker.socket` to a shorter path if `kite up` says so.
- Mailboxes, open requests and the board are rebuilt from the ledger when `kite up` starts, so a
  restart loses neither queued messages nor open requests. Messages handed out but not
  acknowledged come back with `redelivered` incremented, as after a disconnect, and a deadline
  that passed while the broker was down answers `timeout` right after start. Leases do not
  survive a restart. Every start reads the whole ledger; nothing compacts it.
- Identity is the agent name on a socket only the owner can open. It keeps agents apart; it is
  not a defence against another process running as the same user.
- Leases are advisory except where a hook enforces them (the Claude Code hook above).
- Responses are checked against their schema; whether the work they describe was done is not.
- The adapter commands in `kite init`'s comments are examples and have not been tested against
  each host's current CLI flags.

## Layout

| Crate | Holds |
|---|---|
| `kite-proto` | envelopes, error codes, schema registry, `kite.toml`, the wire protocol |
| `kite-core` | the broker, ledger, wait-for graph, socket server, client, supervisor, task-graph runner |
| `kite-cli` | the `kite` binary, the MCP server, the Claude Code hook |

[docs/PROTOCOL.md](docs/PROTOCOL.md) describes the wire protocol and message lifecycle;
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) the concurrency design.

```bash
cargo test
```

The end-to-end suites alone, which run the compiled binary:

```bash
cargo test -p kite-cli --test 'e2e_*'
```

A failing end-to-end test saves its evidence (up.log, ledger, trace, board, status, agent logs)
to `target/tmp/e2e-artifacts/<test>/`.
