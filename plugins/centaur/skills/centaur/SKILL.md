---
name: centaur
description: Use an authenticated Centaur MCP deployment to discover and call team-approved tools. Use when a task requires Centaur, its tool catalog, or the user's Centaur identity and permissions.
---

# Centaur

Use Centaur's MCP tools for actions and context exposed by the user's deployment.

## Workflow

1. When identity or authorization matters, call `centaur_whoami` before other Centaur tools.
2. If the `centaur` MCP tool is listed, use it to list or search the tool catalog when the correct tool is unclear.
3. When `centaur` is available, inspect an unfamiliar CLI progressively with `{"command": ["run", "<tool>", "--help"]}`. Add known command and subcommand segments before `--help` until the relevant options and arguments are shown. Treat that help output as the current contract.
4. When `centaur` is available, run the command with `{"command": ["run", "<tool>", "<argv>", "..."]}`. Pass every CLI argument as a separate token and do not guess command names or options.
5. If `centaur` is not listed or cannot complete the request, use the legacy per-service MCP tools. Each accepts a `method` and `arguments` object. Call it with `method: "help"` when its methods or arguments are unclear.
6. Summarize consequential writes and return relevant identifiers or links.

## Agent sessions

If `centaur_session_send` is listed, use it to hand a multi-step task to a Centaur agent that works in its own sandbox.

1. Call `centaur_session_send` with the prompt. Omit `session_id` to start a new session. The call waits for the turn and returns `final_answer`.
2. If `done` is false in the result, call `centaur_session_read` with the returned `session_id` until `done` is true. Pass `after_event_id` from `next_after_event_id` to read only new progress.
3. Continue the conversation with `centaur_session_send` and the same `session_id`. Use `centaur_session_interrupt` to stop a running turn, and `centaur_session_list` to find earlier sessions.

Centaur authorizes calls using the signed-in principal's live roles and grants. Never request, paste, print, or store Centaur OAuth tokens.

## Connection recovery

If no Centaur tools are available, explain that the client still needs the deployment-specific MCP endpoint.

- Codex: register it with `codex mcp add centaur --url <CENTAUR_MCP_URL>`, then run `codex mcp login centaur`. The `--url` flag is required for Streamable HTTP and OAuth.
- Claude Code: configure the plugin's `mcp_url`, open `/mcp`, and authenticate the `centaur` server.
- Other MCP clients: configure a remote HTTP server named `centaur` with the deployment's `/mcp` URL and complete its OAuth flow.

Do not substitute a guessed hostname. Ask for the deployment URL when it is not already configured or supplied.
