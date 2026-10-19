# Changelog

Kite follows [Semantic Versioning](https://semver.org). Before 1.0, a minor release (0.x) may
break compatibility; breaking changes are listed first. The wire protocol (`kite/1`) and the
ledger format (2) are versioned separately: an incompatible change bumps them, and `kite up`
refuses a ledger written by a newer format.

## 0.2.0 (unreleased)

### Breaking

- **Agents must present their token.** Each agent has `.kite/tokens/<agent>`; registering without
  it is refused with `unauthorized`. Re-run `kite connect <agent>` for each host (its output now
  passes `--token-file`), or set `[broker] require_tokens = false` for throwaway setups.
- **The ledger is a directory.** `[broker] ledger` now names a directory of segments (default
  `.kite/ledger`). A 0.1 ledger file is moved in as `000001.jsonl` on first start; Kite 0.1 would
  then see an empty ledger.
- `kite.toml` may not set `KITE_AGENT`, `KITE_SOCKET`, `KITE_ROOT`, `KITE_TOKEN` or
  `KITE_TOKEN_FILE` in an agent's `env`.
- JSON Schemas in `kite.toml` can no longer `$ref` URLs or files.

### Added

- Per-agent tokens, `kite token show|rotate`, `--token-file` / `KITE_TOKEN_FILE` / `KITE_TOKEN`.
- Ledger segments compacted at startup (`[broker] segment_bytes`), so starts read only the live
  segment; `snapshot` records; ledger format marker.
- Git work for card moves runs off the broker's event loop with a timeout
  (`[board] git_timeout_ms`); a card waiting on git answers `busy`.
- Supervised agents run in their own process group and are stopped as a group
  (`[agents.x] stop_grace_ms`); leftover agents from a killed `kite up` are reported.
- `[broker] max_connections`; silent unregistered connections are closed.
- A torn final ledger record (crash mid-write) is repaired on start and recorded as `recovered`.
- Logs under `KITE_LOG`; MSRV 1.89; license MIT OR Apache-2.0; release builds for Linux and macOS.

### Fixed

- A second `kite up` on a running project interleaved records into its ledger.
- A stop signal during startup could kill `kite up` without a clean shutdown.
- The socket directory could be left open to other users.

## 0.1.0

First version: the broker with checked routes, schemas, deadlines, deadlock refusal, leases and a
hash-chained ledger; the MCP server and CLI; the supervisor; the task-graph runner; the agent
Kanban with gated moves and git worktrees; restart durability.
