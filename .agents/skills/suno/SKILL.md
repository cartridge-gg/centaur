---
name: suno
description: The complete Suno tool — write and generate AI music from the terminal with the `suno` CLI. Use when the user asks to generate/make/create a song, music, a track, or audio; to write a song or lyrics for Suno; for catchy/viral/earworm hooks; for covers, remasters, stems, or voice personas; to download Suno songs (auto-embeds lyrics into MP3); or for priming/subliminal/charm/research songs — "prime [name] to [action]", subliminal song, charm round, research priming, or a song "for the paper". `suno write` scaffolds the song from a built-in grammar, `suno generate` renders the audio. Run `suno agent-info` for the full machine-readable capability dump.
---

# suno CLI

One binary does the whole job — composing the song and rendering the audio. All capability detail lives in the binary so it never drifts from this file.

## Make a song

1. Scaffold a Suno-ready song from the built-in grammar:
   ```bash
   suno write --genre "indie rock" --theme "late-night city drives" --vocal male --viral --out song.txt --json
   ```
   `--out` writes the **lyric block only** (meta-tagged `[Verse]`/`[Chorus]` skeleton with inline `<...>` placeholders), so the file feeds `generate --lyrics-file` directly. The Style Prompt, Suno Tags and `data.next_action` come back in the JSON envelope. `--project-out FILE` saves the composite human document; never pass that one as lyrics.
2. Fill in the `<...>` lyric lines in `song.txt` — replace every angle-bracket span, keep the `[Section]` tags, repeat the chorus verbatim.
3. Render the audio by running `data.next_action.argv` (authoritative — never shell-parse `next_action.command`):
   ```bash
   suno generate --title "..." --tags "<style_prompt>" --lyrics-file song.txt --wait --download ./songs/
   ```
   `generate` exits 3 if any `<...>` placeholder survives, so an unfilled scaffold can never burn credits. It renders on the configured default model — v5.5, Suno's latest (~70 credits) — unless you pass `--model`.

## Priming / research songs

```bash
suno write --mode priming --target "..." --objective "..." --domain investment --subtlety stealth --out song.txt --json
```
Adds a chill-lounge low-arousal scaffold plus a Prime-Stack Map and research-artefact block (in the envelope, not the lyrics file). Priming is consent-based: `--target`, `--objective` and `--domain` are required and a missing one exits 3. Then fill the lyrics and `suno generate` as above.

## Deep reference & everything else

- `suno guide songwriting` — the full grammar (structure, meta-tags, genres, vocal styles, viral hooks)
- `suno guide priming` — consent frame, evidence-graded prime library, phonetic name-embedding, quality gates
- `suno agent-info` — machine-readable manifest: every command, flag, model, exit code, envelope shape, config key
- `suno --help` / `suno <command> --help` — usage, tips, real examples
- First run: `suno auth --login`, then `suno doctor` to verify

Piped output is a JSON envelope automatically — `suno write > song.txt` gets JSON, not lyrics; use `--out`. `suno write` and `suno lyrics` are free; generation ≈70 credits/call on v5.5.
