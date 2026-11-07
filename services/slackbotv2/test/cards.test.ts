import { describe, expect, it } from 'bun:test'
import { CardFenceExtractor, extractCardFences, postCardSpecs, renderCardSpec } from '../src/cards'

const SPEC = JSON.stringify({
  root: 'card',
  elements: {
    card: { type: 'Card', props: { title: 'Installs' }, children: ['kpis', 'table'] },
    kpis: { type: 'Fields', props: {}, children: ['total'] },
    total: { type: 'Field', props: { label: 'Total', value: 1284 }, children: [] },
    table: {
      type: 'Table',
      props: { headers: ['Day', 'Installs'], rows: [['Mon', 600], ['Tue', 684]], align: ['left', 'right'] },
      children: []
    }
  }
})

const ANSWER = `Here are this week's installs.\n\n\`\`\`card\n${SPEC}\n\`\`\`\n\nTuesday was the best day.`

describe('CardFenceExtractor', () => {
  it('withholds a card fence however the answer is split into chunks', () => {
    for (let size = 1; size <= ANSWER.length; size++) {
      const extractor = new CardFenceExtractor()
      let markdown = ''
      for (let offset = 0; offset < ANSWER.length; offset += size) {
        markdown += extractor.push(ANSWER.slice(offset, offset + size))
      }
      markdown += extractor.finish()
      expect(markdown).toBe("Here are this week's installs.\n\n\nTuesday was the best day.")
      expect(extractor.cardSpecs()).toEqual([SPEC])
    }
  })

  it('leaves other code fences and mid-line markers in the markdown', () => {
    const text = 'Use ```card blocks.\n```cards\nnot a card\n```\n```json\n{}\n```'
    expect(extractCardFences(text)).toEqual({ markdown: text, specs: [] })
  })

  it('keeps an unterminated fence as a spec instead of leaking it', () => {
    expect(extractCardFences(`Intro\n\`\`\`card\n${SPEC}`)).toEqual({
      markdown: 'Intro\n',
      specs: [SPEC]
    })
  })
})

describe('renderCardSpec', () => {
  it('builds a Chat SDK card with fields and a table', () => {
    const rendered = renderCardSpec(SPEC)
    expect(rendered.ok).toBe(true)
    if (!rendered.ok) return
    expect(rendered.card.title).toBe('Installs')
    expect(rendered.card.children.map(child => child.type)).toEqual(['fields', 'table'])
  })

  it('rejects buttons that have no action handler', () => {
    const rendered = renderCardSpec(JSON.stringify({
      root: 'card',
      elements: {
        card: { type: 'Card', props: { title: 'Pick' }, children: ['row'] },
        row: { type: 'Actions', props: {}, children: ['go'] },
        go: { type: 'Button', props: { id: 'go', label: 'Go' }, children: [] }
      }
    }))
    expect(rendered).toMatchObject({ ok: false })
  })
})

describe('postCardSpecs', () => {
  it('posts valid specs as cards and invalid specs as their raw JSON', async () => {
    const posted: unknown[] = []
    const result = await postCardSpecs(async message => { posted.push(message) }, [SPEC, '{"root":'])
    expect(result.posted).toBe(2)
    expect(result.rejected).toHaveLength(1)
    expect(posted[0]).toMatchObject({ type: 'card', title: 'Installs' })
    expect(posted[1]).toEqual({ markdown: '```\n{"root":\n```' })
  })
})
