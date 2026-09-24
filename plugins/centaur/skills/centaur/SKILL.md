---
name: centaur
description: Use an authenticated Centaur MCP deployment to discover and call team-approved tools. Use when a task requires Centaur, its tool catalog, or the user's Centaur identity and permissions.
---

# Centaur

Use Centaur's MCP tools for actions and context exposed by the user's deployment.

## Workflow

1. When identity or authorization matters, call `centaur_whoami` before other Centaur tools.
2. Centaur serves Code Mode:
   - Call `centaur_catalog_search` whenever a task involves an external service, private or current data, an external action, or a service URL. Search terms should include service names, URLs, domains, and the requested action; results include each function's signature.
   - Call `centaur_catalog_load` with a connector name when the one-line signature is not enough. Treat the returned TypeScript declarations and their JSDoc as the current contract.
   - Do the work in one `centaur_execute` cell. `code` is the body of an async function: call connectors as `await tools.<connector>.<function>(args)`, run independent calls together with `Promise.all` or `Promise.allSettled` in the same cell instead of several tool calls, and `return` the JSON-serializable value you want back. `console.log` output is returned with the result. A cell has no `fetch`, no module imports, no file access, and no state between cells, and is limited to 120 s, 1000 calls and 8 concurrent calls.
3. Summarize consequential writes and return relevant identifiers or links.

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
