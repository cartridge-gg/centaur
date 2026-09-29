use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand, ValueEnum};
use harness_server::switch::{run_switchable_blocks_server, switching_enabled};
use harness_server::transcript::{ConvertRequest, convert_session};
use harness_server::{
    HarnessKind, Result, run_blocks_server, run_harness_server, run_hermes_blocks_server,
    run_nanocodex_blocks_server,
};
use session_transfer::Tool;
use session_transfer::discover::{Homes, SessionRef, claude_home, codex_home, hermes_home};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Serve agent harnesses over Centaur's streaming protocols."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Debug, Subcommand)]
#[command(rename_all = "kebab-case")]
enum CliCommand {
    Codex(HarnessCommand),
    #[command(alias = "claude")]
    ClaudeCode(HarnessCommand),
    Amp(HarnessCommand),
    /// Run Nanocodex directly as a library and stream its native typed events.
    Nanocodex,
    /// Drive Hermes Agent's long-lived JSON-RPC gateway (sessions, memory,
    /// skills, crons survive across turns).
    Hermes,
    /// Drive Pi's long-lived RPC mode.
    Pi(HarnessCommand),
    /// Drive OMP's long-lived RPC mode with native session continuation.
    Omp(HarnessCommand),
    /// Work with native Codex, Claude Code and Hermes Agent sessions.
    #[command(subcommand)]
    Transcript(TranscriptCommand),
}

#[derive(Debug, Subcommand)]
enum TranscriptCommand {
    /// Convert a session so that the other harness can resume it. Prints JSON.
    Convert(ConvertArgs),
}

#[derive(Debug, Args)]
struct ConvertArgs {
    /// Harness that wrote the session: codex, claude or hermes
    #[arg(long)]
    from: Tool,
    /// Harness to convert to: codex, claude or hermes [default: claude, or
    /// codex for a Claude Code session]. A Hermes session is imported with
    /// the Python in $HERMES_PYTHON, or else python3.
    #[arg(long)]
    to: Option<Tool>,
    /// A session file path, a session id or id prefix, or `latest`
    session: SessionRef,
    /// Project directory of the new session [default: the session's cwd]
    #[arg(long, value_name = "DIR")]
    cwd: Option<String>,
    /// Codex config directory [default: $CODEX_HOME or ~/.codex]
    #[arg(long, value_name = "DIR")]
    codex_home: Option<PathBuf>,
    /// Claude Code config directory [default: $CLAUDE_CONFIG_DIR or ~/.claude]
    #[arg(long, value_name = "DIR")]
    claude_home: Option<PathBuf>,
    /// Hermes Agent home directory, with state.db [default: $HERMES_HOME or ~/.hermes]
    #[arg(long, value_name = "DIR")]
    hermes_home: Option<PathBuf>,
    /// Keep only the last N messages (0 = all)
    #[arg(long, value_name = "N")]
    last: Option<usize>,
    /// Cut each tool call and tool output to N characters (0 = no limit)
    #[arg(long, value_name = "N", default_value_t = 4000)]
    max_output: usize,
    /// Include reasoning summaries as text
    #[arg(long)]
    keep_thinking: bool,
    /// Do not scrub API keys and other secrets
    #[arg(long)]
    no_redact: bool,
    /// model_provider of a converted Codex rollout
    #[arg(long, value_name = "ID")]
    model_provider: Option<String>,
    /// Leave out an unanswered prompt at the end of the session
    #[arg(long)]
    drop_trailing_prompt: bool,
    /// Print the result without writing a file
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Parser)]
struct HarnessCommand {
    #[arg(long, value_enum, default_value_t = ServerMode::Blocks)]
    mode: ServerMode,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ServerMode {
    Blocks,
    Jsonrpc,
}

fn main() {
    exit_on_sigterm();
    if let Err(error) = run() {
        eprintln!("harness-server: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse()
        .command
        .unwrap_or(CliCommand::Codex(HarnessCommand {
            mode: ServerMode::Blocks,
        })) {
        CliCommand::Codex(command) => run_mode(HarnessKind::Codex, command.mode),
        CliCommand::ClaudeCode(command) => run_mode(HarnessKind::ClaudeCode, command.mode),
        CliCommand::Amp(command) => run_mode(HarnessKind::Amp, command.mode),
        CliCommand::Pi(command) => run_mode(HarnessKind::Pi, command.mode),
        CliCommand::Nanocodex => run_nanocodex_blocks_server(),
        CliCommand::Hermes if switching_enabled() => run_switchable_blocks_server(Tool::Hermes),
        CliCommand::Hermes => run_hermes_blocks_server(),
        CliCommand::Omp(command) => run_mode(HarnessKind::Omp, command.mode),
        CliCommand::Transcript(TranscriptCommand::Convert(args)) => run_convert(args),
    }
}

fn run_convert(args: ConvertArgs) -> Result<()> {
    let to = args.to.unwrap_or(match args.from {
        Tool::Claude => Tool::Codex,
        Tool::Codex | Tool::Hermes => Tool::Claude,
    });
    let report = convert_session(&ConvertRequest {
        from: args.from,
        to,
        session: args.session,
        cwd: args.cwd,
        homes: Homes {
            codex: codex_home(args.codex_home),
            claude: claude_home(args.claude_home),
            hermes: hermes_home(args.hermes_home),
        },
        last_messages: args.last.filter(|&n| n > 0),
        max_tool_output: (args.max_output > 0).then_some(args.max_output),
        keep_thinking: args.keep_thinking,
        redact: !args.no_redact,
        codex_model_provider: args.model_provider,
        drop_trailing_prompt: args.drop_trailing_prompt,
        dry_run: args.dry_run,
    })?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

fn run_mode(kind: HarnessKind, mode: ServerMode) -> Result<()> {
    let switchable = match kind {
        HarnessKind::Codex => Some(Tool::Codex),
        HarnessKind::ClaudeCode => Some(Tool::Claude),
        _ => None,
    };
    match (mode, switchable) {
        (ServerMode::Blocks, Some(tool)) if switching_enabled() => {
            run_switchable_blocks_server(tool)
        }
        (ServerMode::Blocks, _) => run_blocks_server(kind),
        (ServerMode::Jsonrpc, _) => run_harness_server(kind),
    }
}

/// harness-server is PID 1 in the sandbox, and the kernel ignores SIGTERM for
/// PID 1 unless it installs a handler. Without one, stopping a sandbox waits
/// out the pod's whole termination grace period before SIGKILL. On SIGTERM,
/// stop the harness processes, flush telemetry, and exit.
fn exit_on_sigterm() {
    std::thread::spawn(|| {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
        else {
            return;
        };
        runtime.block_on(async {
            let Ok(mut terminate) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            else {
                return;
            };
            terminate.recv().await;
            if std::process::id() == 1 {
                stop_harness_processes();
            }
            harness_server::flush_telemetry();
            std::process::exit(143);
        });
    });
}

/// How long harnesses get to finish writing their session state on shutdown;
/// well inside the pod's 30s termination grace period.
const HARNESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Forwards SIGTERM to every other process in the sandbox and reaps them until
/// none are left or the timeout passes. Only valid as PID 1: there, `kill(-1)`
/// reaches exactly the container's other processes.
fn stop_harness_processes() {
    // SAFETY: kill and waitpid take no pointers we own; a null status is allowed.
    unsafe {
        libc::kill(-1, libc::SIGTERM);
    }
    let deadline = Instant::now() + HARNESS_SHUTDOWN_TIMEOUT;
    while Instant::now() < deadline {
        // SAFETY: as above.
        match unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) } {
            // No children left to wait for.
            pid if pid < 0 => return,
            0 => std::thread::sleep(Duration::from_millis(50)),
            _ => {}
        }
    }
}
