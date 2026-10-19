# Working on Kite

- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings` and `cargo test` must pass.
- Broker state has one owner, the broker task. Do not put broker state behind a lock or share it
  with another task; add a command instead.
- Every queue is bounded. A new channel or buffer needs a limit and a defined answer when it is full.
- Every request must end with exactly one answer. A change to request handling needs a test that
  shows the answer still arrives (response, error, timeout, cancel or peer_down).
- Tests do not assert on elapsed time. Use tokio's paused clock (`start_paused = true`) for
  deadlines and leases, and wait for states, not durations.
- Tests exercise the real broker, client and binary, not stand-ins for them.
- Behaviour that crosses a process boundary (CLI, MCP, supervisor, git, restarts) gets an
  end-to-end test in `crates/kite-cli/tests/e2e_*.rs` built on `common::Project`. Wait for
  states with `wait_until`, never a fixed sleep, and give each `Project` a unique name so its
  failure artifacts land in their own `target/tmp/e2e-artifacts/<name>/`.
- Card moves are checked by `kite-core/src/board.rs`; a new move or exit criterion needs a unit
  test there and a row in README.md's Agent Kanban table.
- A change to the wire protocol, an error code, a ledger event or a `kite.toml` key updates
  docs/PROTOCOL.md or README.md in the same change.
