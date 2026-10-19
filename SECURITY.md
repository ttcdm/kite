# Security

Kite runs on one machine for one user. It coordinates agents that are usually LLM-driven, so it
assumes agents make mistakes and sometimes do things they were not asked to; it does not assume
they are hostile programs.

## What Kite protects against

- **An agent acting as another.** Each declared agent has a random 256-bit token in
  `.kite/tokens/<agent>` (mode 0600, directory 0700). Registering needs that token; passing
  another agent's name is refused with `unauthorized`. The token is compared in constant time and
  is never written to the ledger, to logs or to error messages.
- **Agents breaking the contract.** Routes, JSON Schemas, kinds, deadlines and the board's exit
  criteria are enforced by the broker, not by the agents.
- **Other users on the machine.** The socket lives in a directory only its owner can enter;
  `kite up` tightens an over-open directory and refuses one that belongs to someone else.
- **Tokens leaking into commits.** Hosts get the token by file path (`--token-file`), not inline,
  and `kite init` writes `.kite/.gitignore` so nothing under `.kite/` is committed.
- **Schemas reaching outside.** JSON Schema `$ref`s to URLs or files are refused when `kite.toml`
  is loaded; Kite makes no network requests.
- **Tampering with the record.** The ledger is hash-chained; `kite trace --verify` finds the first
  edited, removed or reordered record, and `kite up` refuses to start on a broken chain.

## What it does not protect against

- **A process running as you that reads another agent's token file on purpose.** Anything with
  your user's file access can. Tokens stop accidents and casual impersonation (an agent running
  `kite send --as lead`), not a determined program. Run agents you do not trust as a different
  user or in a sandbox that cannot read `.kite/tokens/`.
- **Agents editing files they have not leased.** Leases are advisory except through the Claude
  Code hook (`kite hook claude-code`), and an agent with a shell can still write files directly.
- **Work that was not done.** The broker checks that a response matches its schema, not that the
  work it describes happened; the board checks that evidence and a handoff note exist, not that
  they are true.
- **Anything over a network.** Kite has no network transport.

## Operating it safely

- Keep `[broker] require_tokens = true` (the default) unless the project is a throwaway.
- After a token may have leaked, run `kite token rotate <agent>` and restart that agent's host.
- Keep `.kite/` out of backups you share; it holds tokens and the full message history.

## Reporting a vulnerability

Please report it privately through GitHub's security advisories on the project's repository
rather than in a public issue.
