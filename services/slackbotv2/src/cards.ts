import type { ChatSDKStreamChunk } from '@centaur/rendering'
import { CardSpecError, standardComponentDefinitions, toCard } from '@json-render/chat'
import type { CardElement } from 'chat'

// Agents ask for a native Slack card by writing a json-render spec in a fenced
// block with this info string. The fence is withheld from the streamed markdown
// and the validated card is posted as its own message once the answer ends.
const CARD_FENCE_OPEN = '```card'
const CARD_FENCE_CLOSE = '\n```'

// Agent-emitted buttons have no handler behind them, so only link buttons are
// accepted; a Button element fails validation and falls back to the raw spec.
const { Button: _button, ...cardComponents } = standardComponentDefinitions
void _button

const CARD_LIMITS = { tables: 1, charts: 2, tableRows: 100, tableColumns: 20 }
const REJECTED_SPEC_MAX_CHARS = 3500

export type RenderedCardSpec =
  | { ok: true; card: CardElement }
  | { ok: false; issues: string[]; source: string }

export function renderCardSpec(source: string): RenderedCardSpec {
  let spec: unknown
  try {
    spec = JSON.parse(source)
  } catch (error) {
    return { ok: false, issues: [`invalid JSON: ${(error as Error).message}`], source }
  }
  try {
    const card = toCard(spec as Parameters<typeof toCard>[0], {
      components: cardComponents,
      limits: CARD_LIMITS
    })
    return { ok: true, card: card as CardElement }
  } catch (error) {
    if (error instanceof CardSpecError) return { ok: false, issues: error.issues, source }
    throw error
  }
}

/**
 * Removes ```card fences from markdown that may arrive split across chunks.
 * Text that could still be the start of a fence is held back until the next
 * chunk proves otherwise.
 */
export class CardFenceExtractor {
  private pending = ''
  private inFence = false
  private skipCloseLine = false
  private atLineStart = true
  private readonly specs: string[] = []

  push(text: string): string {
    this.pending += text
    let out = ''
    for (;;) {
      if (this.skipCloseLine) {
        const lineEnd = this.pending.indexOf('\n')
        this.pending = lineEnd < 0 ? '' : this.pending.slice(lineEnd + 1)
        if (lineEnd < 0) return out
        this.skipCloseLine = false
        this.atLineStart = true
      }
      if (this.inFence) {
        const close = this.pending.indexOf(CARD_FENCE_CLOSE)
        if (close < 0) return out
        this.specs.push(this.pending.slice(0, close).trim())
        this.pending = this.pending.slice(close + CARD_FENCE_CLOSE.length)
        this.inFence = false
        this.skipCloseLine = true
        continue
      }
      const open = this.findOpen()
      if (open >= 0) {
        out += this.emit(this.pending.slice(0, open))
        const lineEnd = this.pending.indexOf('\n', open)
        if (lineEnd < 0) {
          this.pending = this.pending.slice(open)
          return out
        }
        this.pending = this.pending.slice(lineEnd + 1)
        this.inFence = true
        continue
      }
      const hold = this.partialOpenLength()
      out += this.emit(this.pending.slice(0, this.pending.length - hold))
      this.pending = this.pending.slice(this.pending.length - hold)
      return out
    }
  }

  /** Flushes held text; an unterminated fence still counts as a spec. */
  finish(): string {
    this.skipCloseLine = false
    if (this.inFence) {
      this.specs.push(this.pending.trim())
      this.pending = ''
      this.inFence = false
      return ''
    }
    const rest = this.pending
    this.pending = ''
    return this.emit(rest)
  }

  cardSpecs(): string[] {
    return [...this.specs]
  }

  private emit(text: string): string {
    if (text) this.atLineStart = text.endsWith('\n')
    return text
  }

  private findOpen(): number {
    let from = 0
    for (;;) {
      const index = this.pending.indexOf(CARD_FENCE_OPEN, from)
      if (index < 0) return -1
      const lineStart = index === 0 ? this.atLineStart : this.pending[index - 1] === '\n'
      const lineEnd = this.pending.indexOf('\n', index)
      const info = this.pending.slice(index + CARD_FENCE_OPEN.length, lineEnd < 0 ? undefined : lineEnd)
      // Reject ```cards and friends, but accept trailing whitespace. An open
      // line without its newline yet is decided once more text arrives.
      if (lineStart && (/^\s*$/.test(info))) return index
      from = index + 1
    }
  }

  private partialOpenLength(): number {
    const max = Math.min(this.pending.length, CARD_FENCE_OPEN.length - 1)
    for (let length = max; length > 0; length--) {
      const start = this.pending.length - length
      const lineStart = start === 0 ? this.atLineStart : this.pending[start - 1] === '\n'
      if (lineStart && CARD_FENCE_OPEN.startsWith(this.pending.slice(start))) return length
    }
    return 0
  }
}

export function extractCardFences(text: string): { markdown: string; specs: string[] } {
  const extractor = new CardFenceExtractor()
  const markdown = extractor.push(text) + extractor.finish()
  return { markdown, specs: extractor.cardSpecs() }
}

export async function* withheldCardFences(
  stream: AsyncIterable<ChatSDKStreamChunk>,
  extractor: CardFenceExtractor
): AsyncIterable<ChatSDKStreamChunk> {
  for await (const chunk of stream) {
    if (chunk.type !== 'markdown_text') {
      yield chunk
      continue
    }
    const text = extractor.push(chunk.text)
    if (text) yield { type: 'markdown_text', text }
  }
  const rest = extractor.finish()
  if (rest) yield { type: 'markdown_text', text: rest }
}

/**
 * Posts each spec as a native card. A spec that fails validation is posted as
 * its raw JSON so the content the agent produced is never silently dropped.
 */
export async function postCardSpecs(
  post: (message: CardElement | { markdown: string }) => Promise<unknown>,
  specs: string[]
): Promise<{ posted: number; rejected: string[][] }> {
  const rejected: string[][] = []
  let posted = 0
  for (const source of specs) {
    if (!source) continue
    const rendered = renderCardSpec(source)
    if (rendered.ok) {
      await post(rendered.card)
    } else {
      rejected.push(rendered.issues)
      const shown = rendered.source.length > REJECTED_SPEC_MAX_CHARS
        ? `${rendered.source.slice(0, REJECTED_SPEC_MAX_CHARS)}\n[truncated]`
        : rendered.source
      await post({ markdown: `\`\`\`\n${shown}\n\`\`\`` })
    }
    posted += 1
  }
  return { posted, rejected }
}
