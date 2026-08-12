import { describe, expect, it } from 'bun:test'
import {
  fileEventIdempotencyKey,
  slackFileEventTriggerInput,
  triggerFileEventWorkflow,
  type SlackFileEventTriggerDeps
} from '../src/file-event-trigger'
import type { SlackbotV2Options } from '../src/types'

const DEFAULT_TYPES = ['file_shared', 'file_change'] as const

function eventBody(event: Record<string, unknown>, extra: Record<string, unknown> = {}): string {
  return JSON.stringify({
    type: 'event_callback',
    team_id: 'T1',
    event_time: 1_780_000_000,
    event,
    ...extra
  })
}

const FILE_SHARED_BODY = eventBody({
  type: 'file_shared',
  channel_id: 'C123',
  file_id: 'F123',
  file: { id: 'F123' },
  event_ts: '1780000000.100'
})

const FILE_CHANGE_BODY = eventBody({
  type: 'file_change',
  file_id: 'F123',
  file: { id: 'F123' },
  event_ts: '1780000100.200'
})

function options(overrides: Partial<SlackbotV2Options> = {}): SlackbotV2Options {
  return {
    apiUrl: 'http://session.test',
    apiKey: 'test-api-key',
    botToken: 'xoxb-test',
    signingSecret: 'test',
    fileEventWorkflowName: 'file_review',
    ...overrides
  }
}

function deps(file: Record<string, unknown> | null = null): SlackFileEventTriggerDeps & {
  calls: string[]
} {
  const calls: string[] = []
  return {
    calls,
    filesInfo: async (fileId: string) => {
      calls.push(fileId)
      return file
    }
  }
}

type CapturedRequest = { url: string; init: RequestInit }

function captureFetch(
  responses: Response[] = []
): { fetch: SlackbotV2Options['fetch']; requests: CapturedRequest[] } {
  const requests: CapturedRequest[] = []
  return {
    requests,
    fetch: async (input, init) => {
      requests.push({ url: String(input), init: init ?? {} })
      return responses.shift() ?? Response.json({ ok: true, run_id: 'wfr_1', created: true })
    }
  }
}

describe('slackFileEventTriggerInput', () => {
  it('parses file_shared with channel', () => {
    const input = slackFileEventTriggerInput(FILE_SHARED_BODY, DEFAULT_TYPES)
    expect(input).toEqual({
      channelId: 'C123',
      eventTs: '1780000000.100',
      eventType: 'file_shared',
      fileId: 'F123',
      teamId: 'T1'
    })
  })

  it('parses channel-less file_change', () => {
    const input = slackFileEventTriggerInput(FILE_CHANGE_BODY, DEFAULT_TYPES)
    expect(input?.channelId).toBe('')
    expect(input?.eventType).toBe('file_change')
    expect(input?.fileId).toBe('F123')
  })

  it('falls back to event.file.id when file_id is absent', () => {
    const body = eventBody({ type: 'file_shared', file: { id: 'F9' } })
    expect(slackFileEventTriggerInput(body, DEFAULT_TYPES)?.fileId).toBe('F9')
  })

  it('rejects non-file events, other payload types, and invalid JSON', () => {
    expect(
      slackFileEventTriggerInput(eventBody({ type: 'message', text: 'hi' }), DEFAULT_TYPES)
    ).toBeNull()
    expect(
      slackFileEventTriggerInput(
        JSON.stringify({ type: 'url_verification', challenge: 'x' }),
        DEFAULT_TYPES
      )
    ).toBeNull()
    expect(slackFileEventTriggerInput('not json', DEFAULT_TYPES)).toBeNull()
    expect(
      slackFileEventTriggerInput(eventBody({ type: 'file_shared' }), DEFAULT_TYPES)
    ).toBeNull() // no file id anywhere
  })

  it('respects a custom event-type list', () => {
    expect(slackFileEventTriggerInput(FILE_CHANGE_BODY, ['file_shared'])).toBeNull()
  })
})

describe('fileEventIdempotencyKey', () => {
  const input = slackFileEventTriggerInput(FILE_SHARED_BODY, DEFAULT_TYPES)!

  it('is stable and unbucketed by default', () => {
    expect(fileEventIdempotencyKey('file_review', input)).toBe(
      'slack-file-event:file_review:F123'
    )
    expect(fileEventIdempotencyKey('file_review', input, 0)).toBe(
      'slack-file-event:file_review:F123'
    )
  })

  it('buckets by event time when dedupeSeconds is set', () => {
    expect(fileEventIdempotencyKey('file_review', input, 3600)).toBe(
      `slack-file-event:file_review:F123:${Math.floor(1_780_000_000 / 3600)}`
    )
  })

  it('falls back to the unbucketed key when event_ts is unparsable', () => {
    expect(
      fileEventIdempotencyKey('file_review', { ...input, eventTs: '' }, 3600)
    ).toBe('slack-file-event:file_review:F123')
  })
})

describe('triggerFileEventWorkflow', () => {
  it('is disabled without a configured workflow name', () => {
    const { fetch, requests } = captureFetch()
    expect(
      triggerFileEventWorkflow(
        FILE_SHARED_BODY,
        options({ fetch, fileEventWorkflowName: undefined }),
        deps()
      )
    ).toBeNull()
    expect(requests).toEqual([])
  })

  it('POSTs a workflow run with the expected shape', async () => {
    const { fetch, requests } = captureFetch()
    const task = triggerFileEventWorkflow(FILE_SHARED_BODY, options({ fetch }), deps())
    expect(task).not.toBeNull()
    await task

    expect(requests).toHaveLength(1)
    expect(requests[0]!.url).toBe('http://session.test/api/workflows/runs')
    expect(requests[0]!.init.method).toBe('POST')
    const headers = requests[0]!.init.headers as Record<string, string>
    expect(headers['content-type']).toBe('application/json')
    expect(headers.authorization).toBe('Bearer test-api-key')
    const body = JSON.parse(String(requests[0]!.init.body))
    expect(body).toEqual({
      workflow_name: 'file_review',
      input: {
        channel_id: 'C123',
        event_ts: '1780000000.100',
        event_type: 'file_shared',
        file_id: 'F123',
        team_id: 'T1'
      },
      idempotency_key: 'slack-file-event:file_review:F123'
    })
  })

  it('dispatches channel-less file_change events even with a channel allowlist', async () => {
    const { fetch, requests } = captureFetch()
    const task = triggerFileEventWorkflow(
      FILE_CHANGE_BODY,
      options({ fetch, fileEventChannelAllowlist: ['COTHER'] }),
      deps()
    )
    await task
    expect(requests).toHaveLength(1)
    const body = JSON.parse(String(requests[0]!.init.body))
    expect(body.input.channel_id).toBe('')
  })

  it('drops file_shared events outside the channel allowlist', () => {
    const { fetch, requests } = captureFetch()
    expect(
      triggerFileEventWorkflow(
        FILE_SHARED_BODY,
        options({ fetch, fileEventChannelAllowlist: ['COTHER'] }),
        deps()
      )
    ).toBeNull()
    expect(requests).toEqual([])
  })

  it('applies the title-keyword filter through files.info', async () => {
    const matching = deps({ title: 'Huddle notes: 8/12/26 in #general' })
    const { fetch, requests } = captureFetch()
    await triggerFileEventWorkflow(
      FILE_SHARED_BODY,
      options({ fetch, fileEventTitleKeywords: ['huddle notes', 'huddle transcript'] }),
      matching
    )
    expect(matching.calls).toEqual(['F123'])
    expect(requests).toHaveLength(1)

    const nonMatching = deps({ title: 'Q3 planning doc' })
    const { fetch: fetch2, requests: requests2 } = captureFetch()
    await triggerFileEventWorkflow(
      FILE_SHARED_BODY,
      options({ fetch: fetch2, fileEventTitleKeywords: ['huddle notes'] }),
      nonMatching
    )
    expect(requests2).toEqual([])
  })

  it('swallows files.info failures and missing files', async () => {
    const failing: SlackFileEventTriggerDeps = {
      filesInfo: async () => {
        throw new Error('files.info exploded')
      }
    }
    const { fetch, requests } = captureFetch()
    await triggerFileEventWorkflow(
      FILE_SHARED_BODY,
      options({ fetch, fileEventTitleKeywords: ['huddle notes'] }),
      failing
    )
    expect(requests).toEqual([])

    const { fetch: fetch2, requests: requests2 } = captureFetch()
    await triggerFileEventWorkflow(
      FILE_SHARED_BODY,
      options({ fetch: fetch2, fileEventTitleKeywords: ['huddle notes'] }),
      deps(null)
    )
    expect(requests2).toEqual([])
  })

  it('swallows non-2xx workflow API responses', async () => {
    const { fetch } = captureFetch([new Response('nope', { status: 500 })])
    const task = triggerFileEventWorkflow(FILE_SHARED_BODY, options({ fetch }), deps())
    expect(task).not.toBeNull()
    await task // must not reject
  })

  it('ignores non-file payloads', () => {
    const { fetch, requests } = captureFetch()
    expect(
      triggerFileEventWorkflow(
        eventBody({ type: 'message', text: 'hello' }),
        options({ fetch }),
        deps()
      )
    ).toBeNull()
    expect(requests).toEqual([])
  })
})
