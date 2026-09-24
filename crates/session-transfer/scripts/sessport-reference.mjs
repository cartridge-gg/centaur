// Shared setup for running sessport as the reference implementation.
//
// codex2claude differs from sessport on purpose in one place: it also drops
// Codex-injected user messages that sessport treats as prompts (see
// DROPPED_USER_PREFIXES). sessport ignores a record it skips, so running
// sessport on a copy of the rollout without those records gives the output
// that codex2claude must match byte for byte.

// Codex names rollout files in local time. Pin it so the paths are stable.
process.env.TZ = 'UTC';

import crypto from 'node:crypto';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { pathToFileURL } from 'node:url';

// Must match the prefixes that src/codex.rs adds to sessport's list.
export const DROPPED_USER_PREFIXES = ['<recommended_plugins>'];

// These values must match tests/parity.rs and tests/local_sessions.rs.
export const SESSION_ID = 'aaaaaaaa-0000-4000-8000-000000000000';
export const NOW = new Date('2026-09-23T12:00:00.000Z');
export const CLAUDE_HOME = '/claude-home';
export const CODEX_HOME = '/codex-home';
export const FALLBACK_CWD = '/work/no-cwd';

let counter = 0;
/** Restarts the deterministic uuid sequence: 00000000-0000-4000-8000-000000000001, ... */
export function resetUuids() {
  counter = 0;
}
crypto.randomUUID = () => `00000000-0000-4000-8000-${String(++counter).padStart(12, '0')}`;
syncBuiltinESMExports();

/** Imports the built sessport from $SESSPORT_DIR. */
export async function loadSessport() {
  const dir = process.env.SESSPORT_DIR;
  if (!dir) {
    console.error('Set SESSPORT_DIR to a built sessport checkout.');
    process.exit(1);
  }
  return import(pathToFileURL(join(dir, 'dist/index.js')).href);
}

// Same text extraction as sessport's contentText for Codex messages.
function contentText(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .map((c) => {
      if (!c || typeof c !== 'object' || Array.isArray(c)) return '';
      if (c.type === 'input_text' || c.type === 'output_text' || c.type === 'text') return typeof c.text === 'string' ? c.text : '';
      if (c.type === 'input_image') return '[image]';
      return '';
    })
    .filter(Boolean)
    .join('\n');
}

function isDroppedRecord(line) {
  let record;
  try {
    record = JSON.parse(line.trim());
  } catch {
    return false;
  }
  if (!record || typeof record !== 'object' || Array.isArray(record)) return false;
  // Current format keeps the item in `payload`; legacy rollouts put it at the top level.
  const item = record.payload === undefined ? record : record.type === 'response_item' ? record.payload : undefined;
  if (!item || typeof item !== 'object' || item.type !== 'message' || item.role !== 'user') return false;
  const text = contentText(item.content).trimStart();
  return DROPPED_USER_PREFIXES.some((p) => text.startsWith(p));
}

let tempDir;
/**
 * The rollout that sessport must read: `file` itself, or a copy without the
 * records that codex2claude drops on purpose. The copy keeps the file name,
 * because sessport can take the session id from it.
 */
export function referenceInput(file, tool = 'codex') {
  // The differences apply to the Codex reader only.
  if (tool !== 'codex') return file;
  const lines = readFileSync(file, 'utf8').split('\n');
  const kept = lines.filter((line) => !isDroppedRecord(line));
  if (kept.length === lines.length) return file;
  tempDir ??= mkdtempSync(join(tmpdir(), 'codex2claude-reference-'));
  const copy = join(tempDir, basename(file));
  writeFileSync(copy, kept.join('\n'));
  return copy;
}
