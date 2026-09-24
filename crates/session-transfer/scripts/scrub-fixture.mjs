#!/usr/bin/env node
// Turns a recorded Codex or Claude Code session into a fixture that is safe
// to commit. It keeps the structure of every record the converter reads, and
// replaces private content with short placeholders.
//
// Usage:
//   node scripts/scrub-fixture.mjs --tool codex|claude --root <recorded cwd> <in.jsonl> <out.jsonl>
//
// Record a fixture on a throwaway repo, run this script, then read the output
// before you commit it. tests/fixture_privacy.rs checks the result.

import { homedir, userInfo } from 'node:os';
import { readFileSync, writeFileSync } from 'node:fs';
import { parseArgs } from 'node:util';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: { tool: { type: 'string' }, root: { type: 'string' } },
});
const [input, output] = positionals;
if (!['codex', 'claude'].includes(values.tool) || !values.root || !input || !output) {
  console.error('usage: scrub-fixture.mjs --tool codex|claude --root <recorded cwd> <in> <out>');
  process.exit(1);
}

const WORK = '/work/app';
const encode = (path) => path.replace(/[^a-zA-Z0-9]/g, '-');
// Longest first, so that a path is replaced before its parts.
const roots = [values.root, values.root.replace(/^\/private/, '')];
const replacements = [
  ...roots.map((root) => [root, WORK]),
  ...roots.map((root) => [encode(root), encode(WORK)]),
  [homedir(), '/home/user'],
  [encode(homedir()), encode('/home/user')],
];
const user = userInfo().username;

function scrubText(text) {
  let out = text;
  for (const [from, to] of replacements) out = out.split(from).join(to);
  out = out.replace(new RegExp(`\\b${user}\\b`, 'g'), 'user');
  return out.replace(/[\w.+-]+@(?!example\.com)[\w-]+\.[\w.]+/g, 'user@example.com');
}

function scrubDeep(value) {
  if (typeof value === 'string') return scrubText(value);
  if (Array.isArray(value)) return value.map(scrubDeep);
  if (value && typeof value === 'object') {
    // Keys can be paths too, for example the files of a Codex patch.
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [scrubText(k), scrubDeep(v)]));
  }
  return value;
}

/// Keeps the leading tag of injected context, so the readers still classify it.
function placeholder(text) {
  if (text.startsWith('# AGENTS.md instructions')) {
    return `# AGENTS.md instructions for ${WORK}\n\n<INSTRUCTIONS>\nomitted\n</INSTRUCTIONS>`;
  }
  const tag = /^<([a-zA-Z_][\w -]*)>/.exec(text.trimStart());
  return tag ? `<${tag[1]}>\nomitted\n</${tag[1]}>` : 'omitted';
}

const INJECTED = /^\s*(<[a-zA-Z_][\w -]*>|# AGENTS\.md instructions|Caveat: The messages below)/;

function scrubCodex(line) {
  const p = line.payload;
  switch (line.type) {
    case 'session_meta':
      p.base_instructions = { text: 'Base instructions omitted.' };
      if (p.git) p.git = { commit_hash: '0'.repeat(40), branch: p.git.branch ?? 'main' };
      return line;
    case 'turn_context':
      for (const key of ['user_instructions', 'developer_instructions']) {
        if (key in p) p[key] = 'omitted';
      }
      if ('timezone' in p) p.timezone = 'UTC';
      // Deployments configure their own model names.
      if ('model' in p) p.model = 'gpt-5-codex';
      if (p.collaboration_mode?.settings?.model) p.collaboration_mode.settings.model = 'gpt-5-codex';
      return line;
    case 'world_state':
      line.payload = {};
      return line;
    case 'token_usage_record':
      return null;
    case 'event_msg':
      if (p.type === 'token_count') p.rate_limits = null;
      return line;
    case 'response_item':
      if (p.type === 'message' && Array.isArray(p.content)) {
        for (const item of p.content) {
          if (typeof item.text !== 'string') continue;
          if (p.role === 'developer' || p.role === 'system') item.text = 'Developer instructions omitted.';
          else if (p.role === 'user' && INJECTED.test(item.text)) item.text = placeholder(item.text);
        }
      }
      return line;
    default:
      return line;
  }
}

function scrubClaude(line) {
  if (['atis-latch', 'cost-state', 'bridge-session'].includes(line.type)) return null;
  if (line.type === 'attachment') {
    // Attachments carry injected context: instructions, skills, memory, the
    // user's email. `rendered` is the same content as text.
    if (line.attachment) line.attachment = { type: line.attachment.type, content: 'omitted' };
    if ('rendered' in line) line.rendered = 'omitted';
  }
  const content = line.message?.content;
  if (line.type === 'user' && typeof content === 'string' && INJECTED.test(content)) {
    line.message.content = placeholder(content);
  } else if (line.type === 'user' && Array.isArray(content)) {
    for (const block of content) {
      if (block.type === 'text' && typeof block.text === 'string' && INJECTED.test(block.text)) {
        block.text = placeholder(block.text);
      }
    }
  }
  return line;
}

const scrub = values.tool === 'codex' ? scrubCodex : scrubClaude;
const out = [];
for (const raw of readFileSync(input, 'utf8').split('\n')) {
  if (!raw.trim()) continue;
  let line;
  try {
    line = JSON.parse(raw);
  } catch {
    continue;
  }
  const kept = scrub(line);
  if (kept) out.push(JSON.stringify(scrubDeep(kept)));
}
writeFileSync(output, `${out.join('\n')}\n`);
console.log(`wrote ${out.length} records to ${output}`);
