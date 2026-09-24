#!/usr/bin/env node
// Regenerates tests/golden/ from a built sessport checkout.
//
// sessport (https://github.com/lanternsmith/sessport) is the reference
// implementation. Each golden file is the exact Claude Code session that
// sessport writes for one Codex fixture and one set of convert options, with
// the input changes in scripts/sessport-reference.mjs.
//
// Usage:
//   git clone https://github.com/lanternsmith/sessport && cd sessport
//   npm ci --ignore-scripts && ./node_modules/.bin/tsc -p tsconfig.build.json
//   SESSPORT_DIR=/path/to/sessport node scripts/gen-golden.mjs

import { mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
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

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const FIXTURES = join(ROOT, 'tests/fixtures');
const GOLDEN = join(ROOT, 'tests/golden');

if (new Date(0).getTimezoneOffset() !== 0) throw new Error('TZ must be UTC');
const { claudeAdapter, codexAdapter, convertSession } = await loadSessport();

const DIRECTIONS = {
  'codex-to-claude': { from: 'codex', to: 'claude', adapter: codexAdapter },
  'claude-to-codex': { from: 'claude', to: 'codex', adapter: claudeAdapter },
};

// Same option shape the sessport CLI builds for `convert`.
const VARIANTS = {
  default: {},
  'keep-thinking': { keepThinking: true },
  'no-redact': { redact: false },
  'last-5-max-120': { lastMessages: 5, maxToolOutput: 120 },
  'max-output-0': { maxToolOutput: 0 },
  'cwd-override': { cwd: "/tmp/other dir/it's 😀" },
};
// Large sessions get fewer variants to keep the golden files small.
const LARGE_VARIANTS = ['default', 'last-5-max-120'];
const LARGE_BYTES = 100_000;

function fixtures(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const full = join(dir, e.name);
    if (e.isDirectory()) return fixtures(full);
    return e.name.endsWith('.jsonl') ? [full] : [];
  });
}

rmSync(GOLDEN, { recursive: true, force: true });
let written = 0;
for (const [direction, { from, to, adapter }] of Object.entries(DIRECTIONS)) {
  const dir = join(FIXTURES, from);
  for (const file of fixtures(dir).sort()) {
    const session = await adapter.read(referenceInput(file, from));
    if (!session.messages.length) continue;
    const rel = relative(dir, file).replace(/\.jsonl$/, '');
    const large = statSync(file).size > LARGE_BYTES;
    for (const [name, v] of Object.entries(VARIANTS)) {
      if (large && !LARGE_VARIANTS.includes(name)) continue;
      const options = {
        cwd: v.cwd ?? (session.cwd ? undefined : FALLBACK_CWD),
        lastMessages: v.lastMessages,
        maxToolOutput: v.maxToolOutput,
        keepThinking: v.keepThinking ?? false,
        redact: v.redact ?? true,
      };
      resetUuids();
      const result = await convertSession(session, {
        to,
        cwd: options.cwd,
        homes: { claude: CLAUDE_HOME, codex: CODEX_HOME },
        dryRun: true,
        id: SESSION_ID,
        now: NOW,
        prepare: {
          lastMessages: options.lastMessages,
          maxToolOutput: options.maxToolOutput,
          dropThinking: !options.keepThinking,
          redact: options.redact,
        },
      });
      const out = join(GOLDEN, direction, rel);
      mkdirSync(out, { recursive: true });
      writeFileSync(join(out, `${name}.jsonl`), result.contents);
      const meta = {
        direction,
        fixture: relative(ROOT, file),
        options,
        expected: { id: result.id, path: result.path, resumeCommand: result.resumeCommand },
      };
      writeFileSync(join(out, `${name}.meta.json`), `${JSON.stringify(meta, null, 2)}\n`);
      written++;
    }
  }
}
console.log(`wrote ${written} golden files to ${relative(process.cwd(), GOLDEN) || GOLDEN}`);
