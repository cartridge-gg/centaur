#!/usr/bin/env python3
"""Turns sessions of a recorded Hermes Agent database into a fixture that is
safe to commit: a SQL text dump that a test loads into a temporary database.

Usage:
  python3 scripts/scrub-hermes-fixture.py --root <recording dir> \\
      [--replace FROM=TO ...] <state.db> <out.sql> <session id> [<session id> ...]

The dump holds the given sessions and all their child sessions (compaction
continuations and delegated subagents), with the rows of `schema_version`,
`state_meta`, `sessions`, `messages` and `system_prompts` in their original
table schemas. It leaves out the FTS tables and their triggers.

It keeps the structure of every row that the reader reads, and replaces:
- the recording directory: `<root>/workspace*` with /work/app, `<root>/home`
  with /home/user, `<root>/hermes-home*` with /home/user/.hermes, and the rest
  of `<root>` with /work. `--replace` adds more pairs, for example a path that
  the model mistyped;
- system prompts with a placeholder (the hashes are computed again);
- encrypted reasoning with a placeholder.

Record a fixture on a throwaway repo, run this script, then read the output
before you commit it. tests/fixture_privacy.rs checks the result.
"""

import argparse
import hashlib
import json
import re
import sqlite3

TABLES = ["schema_version", "state_meta", "system_prompts", "sessions", "messages"]
PROMPT = "System prompt omitted."
ENCRYPTED = "omitted"


def replacements(root, extra):
    pairs = []
    for base in {root, root.removeprefix("/private")}:
        pairs += [
            (re.compile(re.escape(base) + r"/workspace[\w-]*"), "/work/app"),
            (re.compile(re.escape(base) + r"/hermes-home[\w-]*"), "/home/user/.hermes"),
            (re.compile(re.escape(base) + r"/home\b"), "/home/user"),
        ]
    for pair in extra:
        old, new = pair.split("=", 1)
        pairs += [(re.compile(re.escape(old)), new)]
    for base in {root, root.removeprefix("/private")}:
        pairs += [(re.compile(re.escape(base)), "/work")]
    return pairs


def scrub_text(value, pairs):
    if not isinstance(value, str):
        return value
    for pattern, new in pairs:
        value = pattern.sub(new, value)
    return value


def scrub_reasoning(raw):
    """Keeps the reasoning items and their summaries; drops the encrypted part."""
    try:
        items = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    if not isinstance(items, list):
        return raw
    for item in items:
        if isinstance(item, dict) and "encrypted_content" in item:
            item["encrypted_content"] = ENCRYPTED
    return json.dumps(items, ensure_ascii=False)


def family(src, ids):
    """The given sessions and all their descendants."""
    found = list(dict.fromkeys(ids))
    index = 0
    while index < len(found):
        children = src.execute(
            "SELECT id FROM sessions WHERE parent_session_id = ? ORDER BY started_at",
            (found[index],),
        ).fetchall()
        found += [child for (child,) in children if child not in found]
        index += 1
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--replace", action="append", default=[])
    parser.add_argument("db")
    parser.add_argument("out")
    parser.add_argument("sessions", nargs="+")
    args = parser.parse_args()

    pairs = replacements(args.root.rstrip("/"), args.replace)
    src = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    dst = sqlite3.connect(":memory:")
    for name in TABLES:
        (sql,) = src.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
        ).fetchone()
        dst.execute(sql)

    ids = family(src, args.sessions)
    marks = ",".join("?" * len(ids))

    def copy(table, where="", params=(), change=None):
        cursor = src.execute(f"SELECT * FROM {table} {where}", params)
        columns = [d[0] for d in cursor.description]
        for row in cursor:
            row = dict(zip(columns, (scrub_text(v, pairs) for v in row)))
            if change:
                change(row)
            dst.execute(
                f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' * len(columns))})",
                list(row.values()),
            )

    copy("schema_version")
    copy("state_meta")
    prompt_hash = hashlib.sha256(PROMPT.encode()).hexdigest()
    if src.execute(
        f"SELECT 1 FROM sessions WHERE id IN ({marks}) AND system_prompt_hash IS NOT NULL", ids
    ).fetchone():
        dst.execute("INSERT INTO system_prompts (hash, prompt) VALUES (?, ?)", (prompt_hash, PROMPT))

    def session(row):
        if row.get("system_prompt_hash"):
            row["system_prompt_hash"] = prompt_hash
        if row.get("system_prompt"):
            row["system_prompt"] = PROMPT

    def message(row):
        if row.get("codex_reasoning_items"):
            row["codex_reasoning_items"] = scrub_reasoning(row["codex_reasoning_items"])

    copy("sessions", f"WHERE id IN ({marks}) ORDER BY started_at", ids, session)
    copy("messages", f"WHERE session_id IN ({marks}) ORDER BY id", ids, message)
    dst.commit()
    with open(args.out, "w", encoding="utf-8") as out:
        for line in dst.iterdump():
            out.write(line + "\n")
    count = dst.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    print(f"wrote {len(ids)} sessions and {count} messages to {args.out}")


if __name__ == "__main__":
    main()
