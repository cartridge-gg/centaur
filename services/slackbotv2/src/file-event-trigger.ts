/**
 * Generic Slack file-event → workflow-run trigger.
 *
 * The Chat SDK's event dispatch drops file events (file_shared, file_change,
 * ...) before any callback fires, so this sidecar parses the raw webhook body
 * itself — after the SDK has verified the Slack signature — and, when
 * enabled, creates a durable workflow run through the session API
 * (POST /api/workflows/runs).
 *
 * Deployment-neutral and default-off: the target workflow name, accepted
 * event types, channel allowlist, and file-title keywords all come from
 * configuration (see server.ts). The bot stays timer-free on purpose — any
 * settle/debounce behavior belongs in the target workflow, which can sleep
 * durably. Per-edit event storms collapse at spawn time via the idempotency
 * key `slack-file-event:{workflow}:{file_id}`; with `fileEventDedupeSeconds`
 * unset the key never varies (and the server never expires keys), so a file
 * triggers at most one run, ever.
 */
import { dispatchWorkflowRun } from './session-api'
import type { SlackbotV2Options } from './types'
import { errorMessage, traceLog, traceWarn } from './utils'

const DEFAULT_FILE_EVENT_TYPES = ['file_shared', 'file_change'] as const

const KEYWORD_FILE_FIELDS = ['title', 'name', 'pretty_type'] as const

export type SlackFileEventTriggerInput = {
  channelId: string
  eventTs: string
  eventType: string
  fileId: string
  teamId: string
}

export type SlackFileEventTriggerDeps = {
  filesInfo: (fileId: string) => Promise<Record<string, unknown> | null>
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function firstString(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return ''
}

export function slackFileEventTriggerInput(
  rawBody: string,
  eventTypes: readonly string[]
): SlackFileEventTriggerInput | null {
  let payload: unknown
  try {
    payload = JSON.parse(rawBody)
  } catch {
    return null
  }
  if (!isRecord(payload) || payload.type !== 'event_callback') return null
  const event = payload.event
  if (!isRecord(event)) return null
  const eventType = typeof event.type === 'string' ? event.type : ''
  if (!eventTypes.includes(eventType)) return null
  const file = isRecord(event.file) ? event.file : undefined
  const fileId = firstString(event.file_id, file?.id)
  if (!fileId) return null
  return {
    // file_shared carries channel_id; file_change does not — the target
    // workflow derives the channel from the file's shares in that case.
    channelId: firstString(event.channel_id),
    eventTs: firstString(
      event.event_ts,
      typeof payload.event_time === 'number' ? String(payload.event_time) : undefined
    ),
    eventType,
    fileId,
    teamId: firstString(payload.team_id)
  }
}

export function fileEventIdempotencyKey(
  workflowName: string,
  input: SlackFileEventTriggerInput,
  dedupeSeconds?: number
): string {
  const base = `slack-file-event:${workflowName}:${input.fileId}`
  const bucketSeconds = dedupeSeconds ?? 0
  if (bucketSeconds <= 0) return base
  const eventSeconds = Math.floor(Number.parseFloat(input.eventTs))
  if (!Number.isFinite(eventSeconds)) return base
  return `${base}:${Math.floor(eventSeconds / bucketSeconds)}`
}

/**
 * Returns null when the event is not a matching file event (or the trigger is
 * disabled); otherwise a promise for the caller's waitUntil. The returned
 * promise never rejects — the webhook path must not see a throw from this
 * sidecar.
 */
export function triggerFileEventWorkflow(
  rawBody: string,
  options: SlackbotV2Options,
  deps: SlackFileEventTriggerDeps
): Promise<void> | null {
  const workflowName = options.fileEventWorkflowName?.trim()
  if (!workflowName) return null
  const eventTypes = options.fileEventTypes?.length
    ? options.fileEventTypes
    : DEFAULT_FILE_EVENT_TYPES
  const input = slackFileEventTriggerInput(rawBody, eventTypes)
  if (!input) return null
  const allowlist = options.fileEventChannelAllowlist ?? []
  // Only enforced when the event carries a channel: file_change events have
  // none, and the target workflow applies its own scoping.
  if (allowlist.length && input.channelId && !allowlist.includes(input.channelId)) {
    return null
  }
  return dispatchFileEventWorkflow(workflowName, input, options, deps)
}

async function dispatchFileEventWorkflow(
  workflowName: string,
  input: SlackFileEventTriggerInput,
  options: SlackbotV2Options,
  deps: SlackFileEventTriggerDeps
): Promise<void> {
  try {
    const keywords = (options.fileEventTitleKeywords ?? [])
      .map(keyword => keyword.trim().toLowerCase())
      .filter(Boolean)
    if (keywords.length) {
      const file = await deps.filesInfo(input.fileId)
      if (!file) {
        traceWarn(options, 'slackbotv2_file_event_files_info_missing', undefined, {
          slack_file_id: input.fileId
        })
        return
      }
      const haystack = KEYWORD_FILE_FIELDS.map(key =>
        typeof file[key] === 'string' ? (file[key] as string) : ''
      )
        .join(' ')
        .toLowerCase()
      if (!keywords.some(keyword => haystack.includes(keyword))) {
        traceLog(options, 'slackbotv2_file_event_skipped_keyword_filter', undefined, {
          event_type: input.eventType,
          slack_file_id: input.fileId
        })
        return
      }
    }
    const idempotencyKey = fileEventIdempotencyKey(
      workflowName,
      input,
      options.fileEventDedupeSeconds
    )
    const run = await dispatchWorkflowRun(options, {
      workflow_name: workflowName,
      input: {
        channel_id: input.channelId,
        event_ts: input.eventTs,
        event_type: input.eventType,
        file_id: input.fileId,
        team_id: input.teamId
      },
      idempotency_key: idempotencyKey
    })
    traceLog(options, 'slackbotv2_file_event_workflow_dispatched', undefined, {
      created: run.created ?? null,
      event_type: input.eventType,
      idempotency_key: idempotencyKey,
      run_id: run.run_id ?? null,
      slack_file_id: input.fileId,
      workflow_name: workflowName
    })
  } catch (error) {
    traceWarn(options, 'slackbotv2_file_event_workflow_dispatch_failed', undefined, {
      error: errorMessage(error),
      event_type: input.eventType,
      slack_file_id: input.fileId,
      workflow_name: workflowName
    })
  }
}
