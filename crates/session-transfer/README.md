# session-transfer

Converts Codex CLI sessions and Claude Code sessions into each other, so that
a session can continue on the other harness with its full history.

| Direction | Reads | Writes | Resume with |
|---|---|---|---|
| Codex to Claude | `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl` | `$CLAUDE_CONFIG_DIR/projects/<encoded cwd>/<id>.jsonl` | `claude --resume <id>` |
| Claude to Codex | `$CLAUDE_CONFIG_DIR/projects/<encoded cwd>/<id>.jsonl` | `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<local time>-<id>.jsonl` | `codex resume <id>`, or app-server `thread/resume` |

The crate is a Rust port of [sessport](https://github.com/lanternsmith/sessport)
(MIT, see `NOTICE`). With the same options, the output is the same as
sessport's output, byte for byte, except for the differences below.

## What a conversion keeps

- User prompts and assistant replies, in order. For Claude Code, only the active branch: rewound and subagent messages are left out.
- Tool calls and tool results, as tagged text (`<tool_call>`, `<tool_result>`). They do not become native tool blocks, because the two harnesses have different tool sets, and a model API can reject a foreign tool call.
- A first message that says where the history comes from, and tells the model to check files again before it edits them.

It does not keep:

- Reasoning. Codex encrypts it, and Claude thinking blocks are signed for one provider. `keep_thinking` adds reasoning summaries as text.
- Images. Each image becomes a short text note.
- Injected context: instructions, `AGENTS.md`, environment context, system reminders.

## Use

```rust
use chrono::Local;
use session_transfer::convert::{ConvertOptions, TrailingPrompt, convert};
use session_transfer::discover::{Homes, SessionRef, resolve_session};
use session_transfer::{Target, Tool};

let homes = Homes::from_env();
let session = resolve_session(Tool::Codex, &"latest".parse::<SessionRef>()?, &homes.codex)?;
let target = Target {
    home: homes.claude.clone(),
    cwd: session.cwd.clone().unwrap_or_default(),
    id: uuid::Uuid::new_v4(),
    now: Local::now().fixed_offset(),
};
let options = ConvertOptions {
    // The caller sends the unanswered prompt again as the next turn.
    trailing_prompt: TrailingPrompt::Drop,
    ..ConvertOptions::default()
};
let converted = convert(&session, Tool::Claude, &target, &options)?;
converted.write()?; // never overwrites a file
println!("{}", converted.resume_command);
```

## Differences from sessport

| Topic | sessport | session-transfer |
|---|---|---|
| Name in the output | `[sessport]`, `msg_sessport_…` | `PrepareOptions::brand` (default `session-transfer`) |
| Claude project directory for a cwd longer than 200 characters | The full encoded path, which Claude Code does not read | The first 200 characters, `-`, and a base-36 hash of the cwd, as Claude Code names it |
| A cut inside a surrogate pair (an emoji) | Keeps half of the pair | Stops before the pair |
| Codex `<recommended_plugins>` user message | A prompt: it becomes the first message and the title | Injected context: left out |
| A session that was already converted | Gets a second preamble | Keeps one preamble |
| Compressed `.jsonl.zst` rollouts | Read on Node.js 22.15 or later | Not supported |

## Tests

```sh
cargo test
```

| Test | What it checks |
|---|---|
| `tests/parity.rs` | Every fixture, in both directions, with 6 option sets: the output equals sessport's output in `tests/golden/`. |
| `tests/convert.rs` | Round trips through written files, and the `convert` options. |
| `tests/claude_format.rs` | The output uses only fields that Claude Code writes, as one linear conversation. |
| `tests/fixture_privacy.rs` | Fixtures and goldens hold no home directories, email addresses, account ids or time zones. |
| `tests/local_sessions.rs` | Optional. Compares with sessport on every session on the machine. Nothing is written to the repository. |

To generate the goldens again, or to run the local benchmark, build sessport first:

```sh
git clone https://github.com/lanternsmith/sessport /tmp/sessport
(cd /tmp/sessport && npm ci --ignore-scripts && ./node_modules/.bin/tsc -p tsconfig.build.json)
SESSPORT_DIR=/tmp/sessport node scripts/gen-golden.mjs
SESSPORT_DIR=/tmp/sessport cargo test --release --test local_sessions -- --ignored --nocapture
```

`scripts/sessport-reference.mjs` gives sessport the input changes that match
the differences above, so the goldens contain only documented differences.

## Fixtures

| Fixture | Source |
|---|---|
| `codex/synthetic-edge-cases.jsonl`, `claude/C1A0DE00-…jsonl`, `claude/synthetic-broken-chain.jsonl` | Written by hand for edge cases: secrets, UTF-16 lengths, JavaScript number and key order, legacy records, rewinds, sidechains, broken chains. |
| `codex/recorded-codex-0.154.jsonl`, `claude/19fa6065-…jsonl` | Recorded with Codex CLI 0.154 and Claude Code 2.1.281 on a throwaway repository, then scrubbed. |
| `*/sessport/` | Copied from sessport. |

> **Warning:** Fixtures are public. Record new sessions on a throwaway
> repository, run `scripts/scrub-fixture.mjs`, and read the result before you
> commit it. `tests/fixture_privacy.rs` must pass.
