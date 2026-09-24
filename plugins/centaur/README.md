# Centaur agent plugin

Connect Codex, Claude Code, and other MCP clients to the tools approved for your Centaur principal. Each Centaur deployment has its own MCP URL, normally ending in `/mcp`.

## Running Tools

Deployments with `CENTAUR_MCP_V2_ENABLED=true` expose four stable MCP tools while the catalog behind them changes dynamically:

```json
{"name": "centaur_catalog_search", "arguments": {"query": "slack messages"}}
{"name": "centaur_catalog_load", "arguments": {"tool": "slack"}}
{"name": "centaur_tool_call", "arguments": {"tool": "slack", "argv": ["search", "incident response"]}}
{"name": "centaur_whoami", "arguments": {}}
```

Search the catalog, load the selected tool's current description and top-level CLI help, then call it. `centaur_tool_call` runs the CLI in your principal's sandbox using current Console policy. To inspect a subcommand, call it with the command segments followed by `--help`. Pass each argument as a separate array element. Spaces within values are preserved. Shell expansion, pipes, and redirection are not interpreted.

Call results include `stdout`, `stderr`, `exit_status`, and `timed_out` in both text and structured content. Plain-text help and JSON output are supported. Nonzero exits and timeouts set MCP `isError`. Calls have a 120-second execution timeout. Existing v1 tools with `method` and `arguments` remain callable for cached clients.

## Agent sessions

Deployments with `CENTAUR_MCP_SESSIONS_ENABLED=true` also expose tools that drive a Centaur agent session. The agent runs in its own harness sandbox with your principal's tools and credentials, and it keeps its conversation across prompts.

```json
{"name": "centaur_session_send", "arguments": {"prompt": "Find the flaky test in the api-rs CI run and propose a fix."}}
{"name": "centaur_session_send", "arguments": {"session_id": "<session_id>", "prompt": "Open a draft PR with that fix."}}
{"name": "centaur_session_send", "arguments": {"prompt": "Run the full test suite.", "wait": false}}
{"name": "centaur_session_read", "arguments": {"session_id": "<session_id>", "wait_seconds": 45}}
{"name": "centaur_session_interrupt", "arguments": {"session_id": "<session_id>"}}
{"name": "centaur_session_list", "arguments": {}}
```

By default, `centaur_session_send` waits for the turn to finish and returns `final_answer`. A prompt sent while a turn runs steers that turn.

- Clients that accept `text/event-stream` get the response as a stream. Each agent step is sent as an MCP progress notification with a one-line `message` (for example `command: gh pr list → exit 0`), and a heartbeat follows every 30 s of silence. Claude Code shows the latest line under the tool call; Codex shows no progress. The stream follows the turn for up to 30 minutes.
- Other clients wait up to 50 s.
- If the result has `done: false`, call `centaur_session_read` until `done` is true. It waits up to `wait_seconds` (at most 50). Pass `after_event_id` from `next_after_event_id` to read only new progress.
- Set `wait: false` to return at once with `session_id` and `execution_id`. In Codex, this plus repeated `centaur_session_read` calls is the way to see progress while a turn runs.

Sessions are private to the principal that started them. For each tool, its arguments and results, and what Claude Code and Codex show during a call, see [Agent Sessions over MCP](../../docs/pages/operate/mcp-agent-sessions.mdx).

## Codex

Add this repository as a marketplace and install the plugin:

```bash
codex plugin marketplace add paradigmxyz/centaur
codex plugin add centaur@centaur
```

Register the deployment as Streamable HTTP and complete OAuth:

```bash
codex mcp add centaur --url <CENTAUR_MCP_URL>
codex mcp login centaur
```

If `centaur` is already registered with the wrong transport, replace it:

```bash
codex mcp remove centaur
codex mcp add centaur --url <CENTAUR_MCP_URL>
codex mcp login centaur
```

Start a new Codex task after installation so it loads the plugin skill.

## Claude Code

Add the marketplace and install the plugin with your deployment URL:

```bash
claude plugin marketplace add paradigmxyz/centaur
claude plugin install centaur@centaur --config mcp_url=<CENTAUR_MCP_URL>
```

Start Claude Code, open `/mcp`, and authenticate `centaur`. The plugin supplies the remote HTTP configuration and Claude stores OAuth credentials outside the plugin.

For local development, validate and load this checkout directly:

```bash
claude plugin validate --strict ./plugins/centaur
claude --plugin-dir ./plugins/centaur
```

## Other MCP clients

Configure a remote Streamable HTTP server using the deployment-specific URL:

```json
{
  "mcpServers": {
    "centaur": {
      "type": "http",
      "url": "<CENTAUR_MCP_URL>"
    }
  }
}
```

Complete OAuth in the client, then verify that `centaur_whoami` returns the expected principal before performing sensitive actions.
