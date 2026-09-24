#!/usr/bin/env node
// Renders sessions with sessport, using the default convert options and the
// fixed id, clock and uuid sequence from scripts/sessport-reference.mjs.
// tests/local_sessions.rs uses it as the benchmark.
//
// Usage: SESSPORT_DIR=/path/to/sessport node scripts/sessport-render.mjs <codex|claude> <out-dir> <session.jsonl>...
// Reads each input as the given tool, converts it to the other tool, and
// writes <out-dir>/<index>.jsonl in argument order. A session with no
// messages gets an empty file.

import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  CLAUDE_HOME,
  CODEX_HOME,
  FALLBACK_CWD,
  NOW,
  SESSION_ID,
  loadSessport,
  referenceInput,
  resetUuids,
} from './sessport-reference.mjs';

const [from, outDir, ...files] = process.argv.slice(2);
if (!['codex', 'claude'].includes(from) || !outDir) {
  console.error('usage: SESSPORT_DIR=<sessport checkout> node scripts/sessport-render.mjs <codex|claude> <out-dir> <session.jsonl>...');
  process.exit(1);
}
const { claudeAdapter, codexAdapter, convertSession } = await loadSessport();
const adapter = from === 'codex' ? codexAdapter : claudeAdapter;
const to = from === 'codex' ? 'claude' : 'codex';

mkdirSync(outDir, { recursive: true });
for (const [index, file] of files.entries()) {
  const session = await adapter.read(referenceInput(file, from));
  let contents = '';
  if (session.messages.length) {
    resetUuids();
    const result = await convertSession(session, {
      to,
      cwd: session.cwd ? undefined : FALLBACK_CWD,
      homes: { claude: CLAUDE_HOME, codex: CODEX_HOME },
      dryRun: true,
      id: SESSION_ID,
      now: NOW,
      prepare: { dropThinking: true, redact: true },
    });
    contents = result.contents;
  }
  writeFileSync(join(outDir, `${index}.jsonl`), contents);
}
