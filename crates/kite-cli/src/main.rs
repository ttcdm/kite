//! `kite`: run the broker, talk to it from a shell, or serve it to an agent host over MCP.

mod hook;
mod init;
mod mcp;
mod paths;
mod show;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use kite_core::runner::{self, Plan};
use kite_core::supervisor::Supervisor;
use kite_core::{Broker, Client, Ledger, ledger, server};
use kite_proto::board::{Blocker, Card, CardDraft, CardOp, CardPatch, CardState};
use kite_proto::wire::{AgentError, AwaitOk};
use kite_proto::{Draft, Kind, KiteError};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;

#[derive(Parser)]
#[command(name = "kite", version, about = "Agent orchestration with strictly checked messages between agents")]
struct Cli {
    /// kite.toml to use. Default: the nearest one at or above the current directory.
    #[arg(long, global = true, env = "KITE_CONFIG")]
    config: Option<PathBuf>,
    /// Print machine-readable JSON.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write a starter kite.toml, schemas/ and .kite/mcp/<agent>.json in the current directory.
    Init {
        #[arg(long)]
        force: bool,
    },
    /// Print how to connect an agent host (Claude Code, Codex, Gemini CLI, Cursor, ...) as AGENT.
    Connect { agent: String },
    /// Run the broker in the foreground and launch agents that declare a command.
    Up {
        /// Do not launch agent commands.
        #[arg(long)]
        no_supervise: bool,
    },
    /// Every agent's state; with AGENT, everything about that one agent.
    Status {
        agent: Option<String>,
        /// Redraw every second until interrupted.
        #[arg(long)]
        watch: bool,
    },
    /// What AGENT may send, to whom, and the schemas involved.
    Contracts {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
    },
    /// Send an event, or with --request a request, and print the result.
    Send {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        schema: String,
        /// JSON, or @file.json.
        #[arg(long)]
        payload: String,
        /// Wait for the answer (exit 0 answered, 1 error answer, 3 still pending).
        #[arg(long)]
        request: bool,
        /// Seconds to wait for the answer with --request.
        #[arg(long, default_value_t = 60)]
        wait: u64,
        #[arg(long)]
        timeout_ms: Option<u64>,
        /// Idempotency key: resending the same id delivers once.
        #[arg(long)]
        id: Option<Ulid>,
    },
    /// Print messages for AGENT and acknowledge them (unless --keep).
    Recv {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        /// Seconds to wait if the mailbox is empty.
        #[arg(long, default_value_t = 0)]
        wait: u64,
        #[arg(long, default_value_t = 16)]
        max: usize,
        /// Do not acknowledge; the messages are handed out again next time.
        #[arg(long)]
        keep: bool,
    },
    /// Answer a request AGENT received.
    Respond {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        #[arg(long)]
        request: Ulid,
        /// JSON, or @file.json.
        #[arg(long, conflicts_with_all = ["error_code", "error_message"])]
        result: Option<String>,
        #[arg(long, requires = "error_message")]
        error_code: Option<String>,
        #[arg(long, requires = "error_code")]
        error_message: Option<String>,
    },
    /// Withdraw a request AGENT sent.
    Cancel {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        #[arg(long)]
        request: Ulid,
    },
    /// Take or renew an exclusive lease. Leases end when their holder disconnects,
    /// so this one ends when the command exits unless --hold keeps it open.
    Lease {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        resource: String,
        #[arg(long, default_value_t = 600)]
        ttl: u64,
        #[arg(long)]
        hold: bool,
    },
    /// Who holds a lease.
    LeaseCheck { resource: String },
    /// Run a task graph (a plan file) against the running broker.
    Run { plan: PathBuf },
    /// Print the ledger, optionally for one agent, or verify its hash chain.
    Trace {
        #[arg(long)]
        agent: Option<String>,
        #[arg(long)]
        verify: bool,
    },
    /// Serve the kite_* tools over MCP on stdio, as AGENT.
    Mcp {
        #[arg(long, env = "KITE_AGENT")]
        agent: Option<String>,
    },
    /// The agent Kanban control pane: cards by column, owners and heartbeats,
    /// blockers by owner, and merge readiness by gate.
    Board {
        /// Also list merged and archived cards.
        #[arg(long)]
        all: bool,
        /// Redraw every second until interrupted.
        #[arg(long)]
        watch: bool,
    },
    /// Create, change, move or record evidence on a card.
    Card {
        #[command(subcommand)]
        action: CardCmd,
    },
    /// Hook entry points for agent hosts.
    Hook {
        #[command(subcommand)]
        host: HookHost,
    },
}

#[derive(Subcommand)]
enum CardCmd {
    /// Create a card in the backlog.
    New {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        #[arg(long)]
        title: String,
        /// An acceptance criterion; repeat for more.
        #[arg(long = "accept")]
        acceptance: Vec<String>,
        #[arg(long)]
        merge_gate: Option<String>,
        /// Comma-separated gates, in order. Default: [board] default_gates.
        #[arg(long, value_delimiter = ',')]
        gates: Option<Vec<String>>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        worktree: Option<String>,
        #[arg(long)]
        id: Option<String>,
    },
    /// Everything on one card.
    Show { id: String },
    /// Change a card's fields; only the ones given change.
    Set {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        id: String,
        #[arg(long)]
        title: Option<String>,
        /// Replaces the acceptance criteria; repeat for more.
        #[arg(long = "accept")]
        acceptance: Vec<String>,
        #[arg(long)]
        merge_gate: Option<String>,
        #[arg(long, value_delimiter = ',')]
        gates: Option<Vec<String>>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        worktree: Option<String>,
        #[arg(long)]
        handoff: Option<String>,
    },
    /// Move a card to another column; refused until its exit criteria are met.
    Move {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        id: String,
        /// backlog, ready, running, review, blocked, merged or archived.
        to: CardState,
        /// Why it is archived.
        #[arg(long)]
        reason: Option<String>,
        /// The commit or PR it merged in.
        #[arg(long)]
        merge_ref: Option<String>,
        #[arg(long)]
        blocker_reason: Option<String>,
        #[arg(long)]
        blocker_owner: Option<String>,
        #[arg(long)]
        next_action: Option<String>,
    },
    /// Record evidence, or with --gate a gate result (--pass or --fail).
    Evidence {
        #[arg(long = "as", env = "KITE_AGENT")]
        agent: String,
        id: String,
        /// test, diff, screenshot, log, review, eval, ...
        #[arg(long)]
        kind: String,
        /// Where it is: a path, URL, commit or command.
        #[arg(long = "ref")]
        reference: String,
        #[arg(long)]
        note: Option<String>,
        #[arg(long)]
        gate: Option<String>,
        #[arg(long, conflicts_with = "fail")]
        pass: bool,
        #[arg(long)]
        fail: bool,
    },
}

impl CardCmd {
    fn agent(&self) -> Option<&str> {
        match self {
            CardCmd::New { agent, .. }
            | CardCmd::Set { agent, .. }
            | CardCmd::Move { agent, .. }
            | CardCmd::Evidence { agent, .. } => Some(agent),
            CardCmd::Show { .. } => None,
        }
    }

    fn into_op(self) -> CardOp {
        match self {
            CardCmd::New { title, acceptance, merge_gate, gates, owner, branch, worktree, id, .. } => {
                CardOp::Create { card: CardDraft { id, title, acceptance, merge_gate, gates, owner, branch, worktree } }
            }
            CardCmd::Show { id } => CardOp::Get { id },
            CardCmd::Set { id, title, acceptance, merge_gate, gates, owner, branch, worktree, handoff, .. } => {
                let acceptance = (!acceptance.is_empty()).then_some(acceptance);
                CardOp::Update {
                    id,
                    patch: CardPatch { title, acceptance, merge_gate, gates, owner, branch, worktree, handoff },
                }
            }
            CardCmd::Move { id, to, reason, merge_ref, blocker_reason, blocker_owner, next_action, .. } => {
                let blocker =
                    (blocker_reason.is_some() || blocker_owner.is_some() || next_action.is_some()).then(|| Blocker {
                        reason: blocker_reason.unwrap_or_default(),
                        owner: blocker_owner.unwrap_or_default(),
                        next_action: next_action.unwrap_or_default(),
                    });
                CardOp::Move { id, to, blocker, reason, merge_ref }
            }
            CardCmd::Evidence { id, kind, reference, note, gate, pass, fail, .. } => {
                let pass = if pass {
                    Some(true)
                } else if fail {
                    Some(false)
                } else {
                    None
                };
                CardOp::Evidence { id, kind, reference, note, gate, pass }
            }
        }
    }
}

#[derive(Subcommand)]
enum HookHost {
    /// Claude Code PreToolUse: block edits to files another agent has leased.
    ClaudeCode {
        #[arg(long, env = "KITE_AGENT")]
        agent: Option<String>,
        /// Also block edits to files this agent has not leased.
        #[arg(long)]
        strict: bool,
    },
}

fn payload(arg: &str) -> anyhow::Result<Value> {
    let text = match arg.strip_prefix('@') {
        Some(file) => std::fs::read_to_string(file).with_context(|| format!("reading {file}"))?,
        None => arg.to_string(),
    };
    serde_json::from_str(&text).context("payload is not valid JSON")
}

fn print(json_mode: bool, v: &Value, human: impl FnOnce() -> String) {
    if json_mode {
        println!("{}", serde_json::to_string_pretty(v).expect("serializes"));
    } else {
        print!("{}", human());
    }
}

async fn agent_client(socket: &Path, agent: &str) -> anyhow::Result<Client> {
    let c = Client::connect(socket).await?;
    c.register(agent).await?;
    Ok(c)
}

fn kite_exe() -> String {
    std::env::current_exe().map(|p| p.display().to_string()).unwrap_or_else(|_| "kite".into())
}

async fn up(paths: &paths::Paths, config: kite_proto::Config, no_supervise: bool) -> anyhow::Result<()> {
    let config = Arc::new(config);
    // Claim the socket before touching the ledger: a second `kite up` on a live
    // project must fail without writing anything.
    let listener = server::bind(&paths.socket).await?;
    let ledger = Ledger::open(&config.broker.ledger, config.broker.fsync)?;
    let (handle, mut broker) = Broker::spawn(config.clone(), ledger);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(server::serve(listener, paths.socket.clone(), handle.clone(), async {
        let _ = stop_rx.await;
    }));
    let sup = if no_supervise {
        None
    } else {
        Some(Supervisor::start(&config, handle.clone(), &config.root.join(".kite/logs"))?)
    };
    eprintln!(
        "kite up: {} agents, {} launched; socket {}; ledger {}",
        config.agents.len(),
        sup.as_ref().map_or(0, Supervisor::len),
        paths.socket.display(),
        config.broker.ledger.display()
    );
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let died = tokio::select! {
        _ = tokio::signal::ctrl_c() => None,
        _ = term.recv() => None,
        r = &mut broker => Some(r),
    };
    if let Some(s) = sup {
        s.stop().await;
    }
    let _ = stop_tx.send(());
    let _ = server.await;
    match died {
        Some(r) => {
            r??;
            bail!("the broker stopped unexpectedly");
        }
        None => {
            handle.shutdown().await;
            broker.await??;
            eprintln!("kite up: stopped");
        }
    }
    Ok(())
}

fn init_here(force: bool) -> anyhow::Result<()> {
    let dir = std::env::current_dir()?;
    let written = init::write(&dir, force)?;
    let config = dir.join("kite.toml");
    let parsed = kite_proto::Config::load(&config).map_err(anyhow::Error::msg)?;
    let mcp_dir = dir.join(".kite/mcp");
    std::fs::create_dir_all(&mcp_dir)?;
    for agent in parsed.agents.keys() {
        let body = serde_json::to_string_pretty(&init::mcp_json(&kite_exe(), agent, &config))?;
        std::fs::write(mcp_dir.join(format!("{agent}.json")), body)?;
    }
    println!("wrote {} and .kite/mcp/<agent>.json for {} agents\n", written.join(", "), parsed.agents.len());
    println!("Next: `kite up` in one terminal, then connect each agent host.\n");
    let first = parsed.agents.keys().next().map_or("lead", |s| s.as_str());
    print!("{}", init::connect_text(&kite_exe(), first, &config));
    Ok(())
}

fn trace(config: &kite_proto::Config, agent: Option<&str>, verify: bool, json_mode: bool) -> anyhow::Result<()> {
    let path = &config.broker.ledger;
    if verify {
        let r = ledger::verify(path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let v = json!({ "ok": true, "records": r.records, "last_hash": r.last_hash });
        print(json_mode, &v, || format!("ledger ok: {} records, chain intact, head {}\n", r.records, r.last_hash));
        return Ok(());
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let rec: Value = serde_json::from_str(line)?;
        if agent.is_some_and(|a| !show::mentions(&rec["event"], a)) {
            continue;
        }
        if json_mode {
            println!("{line}");
        } else {
            println!("{}", show::trace_line(&rec));
        }
    }
    Ok(())
}

async fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    let json_mode = cli.json;
    if let Cmd::Init { force } = cli.cmd {
        init_here(force)?;
        return Ok(ExitCode::SUCCESS);
    }
    let (paths, config) = paths::find(cli.config.as_deref())?;
    match cli.cmd {
        Cmd::Init { .. } => unreachable!("handled above"),
        Cmd::Trace { agent, verify } => trace(&config, agent.as_deref(), verify, json_mode)?,
        Cmd::Connect { agent } => {
            if !config.agents.contains_key(&agent) {
                bail!("{agent:?} is not declared under [agents] in {}", paths.config.display());
            }
            let v = init::mcp_json(&kite_exe(), &agent, &paths.config);
            print(json_mode, &v, || init::connect_text(&kite_exe(), &agent, &paths.config));
        }
        Cmd::Up { no_supervise } => up(&paths, config, no_supervise).await?,
        Cmd::Status { agent, watch } => {
            let c = Client::connect(&paths.socket).await?;
            loop {
                let s = c.status(agent.as_deref()).await?;
                if watch && !json_mode {
                    print!("\x1b[2J\x1b[H");
                }
                print(json_mode, &serde_json::to_value(&s)?, || show::status(&s));
                if !watch {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = tokio::signal::ctrl_c() => break,
                }
            }
        }
        Cmd::Contracts { agent } => {
            let c = agent_client(&paths.socket, &agent).await?;
            println!("{}", serde_json::to_string_pretty(&c.contracts().await?)?);
        }
        Cmd::Send { agent, to, schema, payload: p, request, wait, timeout_ms, id } => {
            let c = agent_client(&paths.socket, &agent).await?;
            let draft = Draft {
                id,
                to,
                schema: schema.parse()?,
                kind: None,
                payload: payload(&p)?,
                timeout_ms,
                blocking: false,
            };
            if !request {
                let sent = c.send(draft).await?;
                let v = serde_json::to_value(&sent)?;
                print(json_mode, &v, || {
                    format!("sent {:?} {} (seq {}, lc {})\n", sent.kind, sent.id, sent.seq, sent.lc)
                });
                return Ok(ExitCode::SUCCESS);
            }
            let (sent, got) = c.request(draft, Duration::from_secs(wait)).await?;
            println!("{}", serde_json::to_string_pretty(&json!({ "request_id": sent.id, "answer": got }))?);
            return Ok(match got {
                AwaitOk::Done { response } if response.kind == Kind::Response => ExitCode::SUCCESS,
                AwaitOk::Done { .. } => ExitCode::from(1),
                AwaitOk::Pending { .. } => ExitCode::from(3),
            });
        }
        Cmd::Recv { agent, wait, max, keep } => {
            let c = agent_client(&paths.socket, &agent).await?;
            let got = c.recv(wait.saturating_mul(1000), max).await?;
            if !keep && !got.messages.is_empty() {
                c.ack(got.messages.iter().map(|m| m.id).collect()).await?;
            }
            let v = serde_json::to_value(&got)?;
            print(json_mode, &v, || {
                let mut out = String::new();
                for m in &got.messages {
                    let corr = m.correlation_id.map(|c| format!(" re {c}")).unwrap_or_default();
                    let body = serde_json::to_string_pretty(&m.payload).expect("serializes");
                    out.push_str(&format!("{} {:?} {} from {}{corr}\n{body}\n\n", m.id, m.kind, m.schema, m.from));
                }
                out.push_str(&format!("{} received, {} still waiting\n", got.messages.len(), got.remaining));
                out
            });
        }
        Cmd::Respond { agent, request, result, error_code, error_message } => {
            let c = agent_client(&paths.socket, &agent).await?;
            let v = match (result, error_code, error_message) {
                (Some(r), None, None) => c.respond(request, payload(&r)?).await?,
                (None, Some(code), Some(message)) => {
                    c.respond_error(request, AgentError { code, message, detail: None }).await?
                }
                _ => bail!("give --result, or --error-code with --error-message"),
            };
            print(json_mode, &v, || format!("answered {request}\n"));
        }
        Cmd::Cancel { agent, request } => {
            let c = agent_client(&paths.socket, &agent).await?;
            let v = c.cancel(request).await?;
            print(json_mode, &v, || format!("cancelled {request}\n"));
        }
        Cmd::Lease { agent, resource, ttl, hold } => {
            let c = agent_client(&paths.socket, &agent).await?;
            let key = paths::resource_key(&paths.root, &resource);
            let l = c.lease(&key, ttl.saturating_mul(1000)).await?;
            let v = serde_json::to_value(&l)?;
            print(json_mode, &v, || {
                format!("{} leased to {} until {}\n", l.resource, l.holder, show::clock(l.expires_at_ms))
            });
            if hold {
                eprintln!("holding; interrupt to release");
                tokio::signal::ctrl_c().await?;
            }
        }
        Cmd::LeaseCheck { resource } => {
            let c = Client::connect(&paths.socket).await?;
            let r = c.lease_check(&paths::resource_key(&paths.root, &resource)).await?;
            let v = serde_json::to_value(&r)?;
            print(json_mode, &v, || match &r.lease {
                Some(l) => format!("{} is leased by {} until {}\n", r.resource, l.holder, show::clock(l.expires_at_ms)),
                None => format!("{} is not leased\n", r.resource),
            });
        }
        Cmd::Run { plan } => {
            let text = std::fs::read_to_string(&plan).with_context(|| format!("reading {}", plan.display()))?;
            let plan = Plan::parse(&text).map_err(|e| anyhow::anyhow!("{}: {e}", plan.display()))?;
            let c = Arc::new(agent_client(&paths.socket, &plan.as_agent).await?);
            let report = runner::run(c, &plan).await.map_err(anyhow::Error::msg)?;
            let v = serde_json::to_value(&report)?;
            print(json_mode, &v, || {
                let mut out = String::new();
                for (id, t) in &report.tasks {
                    let detail = t.error.as_ref().or(t.result.as_ref()).map(|v| v.to_string()).unwrap_or_default();
                    let state = format!("{:?}", t.state).to_lowercase();
                    out.push_str(&format!("{id:<20} {state:<10} {detail}\n"));
                }
                out.push_str(if report.ok { "all tasks done\n" } else { "some tasks did not finish\n" });
                out
            });
            return Ok(if report.ok { ExitCode::SUCCESS } else { ExitCode::from(1) });
        }
        Cmd::Board { all, watch } => {
            let c = Client::connect(&paths.socket).await?;
            loop {
                let cards = c.cards().await?;
                let agents = c.status(None).await?.agents;
                if watch && !json_mode {
                    print!("\x1b[2J\x1b[H");
                }
                let v = json!({ "cards": cards, "agents": agents });
                print(json_mode, &v, || show::board(&cards, &agents, all));
                if !watch {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = tokio::signal::ctrl_c() => break,
                }
            }
        }
        Cmd::Card { action } => {
            let c = match action.agent() {
                Some(a) => agent_client(&paths.socket, a).await?,
                None => Client::connect(&paths.socket).await?,
            };
            let v = c.card(action.into_op()).await?;
            let card: Card = serde_json::from_value(v.clone())?;
            print(json_mode, &v, || show::card(&card));
        }
        Cmd::Mcp { agent } => mcp::serve(agent, paths).await?,
        Cmd::Hook { host: HookHost::ClaudeCode { agent, strict } } => {
            return Ok(ExitCode::from(hook::run(&paths, agent.as_deref(), strict).await as u8));
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("KITE_LOG").unwrap_or_else(|_| "warn".into()))
        .init();
    let cli = Cli::parse();
    let json_mode = cli.json;
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            match e.downcast_ref::<KiteError>() {
                Some(k) if json_mode => println!("{}", json!({ "error": k })),
                Some(k) => {
                    eprintln!("error: {}: {}", k.code, k.message);
                    if let Some(d) = &k.detail {
                        eprintln!("{}", serde_json::to_string_pretty(d).expect("serializes"));
                    }
                }
                None => eprintln!("error: {e:#}"),
            }
            ExitCode::from(1)
        }
    }
}
