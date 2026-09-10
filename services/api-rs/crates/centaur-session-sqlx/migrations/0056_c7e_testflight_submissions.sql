-- TestFlight feedback submissions for the Cartridge overlay
-- (cartridge-gg/agent, docs/dango-feedback-pipeline.md "Recovery").
--
-- One row per TestFlight submission the overlay's feedback pipeline has seen:
-- the parsed inbox comment (tester text, screenshots, device), where the
-- inbox comment and the Slack post live, and the triage state the overlay's
-- recovery scan (`c7e_dango_feedback_recovery_scan`) queries by: status,
-- attempts, the failure reason and its timestamps, and the dispatch that
-- last claimed it. The overlay's webhook feature writes the row at ingest,
-- its triage projects every state-document save into it, and the recovery
-- scan reads it with one query.
--
-- The per-submission state DOCUMENT in company_context_documents stays: it
-- is the claim's advisory-lock target and what the issue resolver, the
-- PostHog loop's shared machinery and the pipeline-monitor board read. This
-- table is the submission registry and the query index, written from the
-- same code path (`_c7e_dango_feedback_common.project_submission_state`).
--
-- Fork-only migration (like 0055): no upstream table is touched. The
-- backfill seeds a row for every submission that already has a state
-- document, so earlier failures are visible the moment api-rs starts with
-- this migration; the tester text and screenshots of those rows are null and
-- the scan re-reads the inbox comment for them.

create table if not exists c7e_testflight_submissions (
  submission_id       text primary key,
  repo                text        not null default '',
  kind                text        not null default 'feedback',
  comment             text,                                   -- tester text (null: not captured)
  screenshots         jsonb,                                  -- list of signed Apple urls
  device_model        text        not null default '',
  os_version          text        not null default '',
  app_platform        text        not null default '',
  build_bundle_id     text        not null default '',
  build_id            text        not null default '',
  tester_id           text        not null default '',
  submitted_at        text        not null default '',        -- Apple's createdDate, as posted
  inbox_issue_number  integer,
  inbox_comment_id    bigint,
  inbox_comment_url   text        not null default '',
  slack_channel_id    text        not null default '',
  slack_message_ts    text        not null default '',
  slack_permalink     text        not null default '',
  status              text        not null default '',        -- '' | in_flight | failed | filed | not_filed
  attempts            integer     not null default 0,
  run_id              text        not null default '',        -- the run that last claimed it
  dispatch_id         text        not null default '',        -- recovery re-dispatch that superseded a failure
  claimed_at          timestamptz,
  first_failed_at     timestamptz,
  last_failed_at      timestamptz,
  last_dispatched_at  timestamptz,                            -- the recovery scan's last re-dispatch
  reason              text        not null default '',
  verdict             text        not null default '',
  issue_url           text        not null default '',
  issue_urls          jsonb       not null default '[]'::jsonb,
  received_at         timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

create index if not exists c7e_testflight_submissions_status_idx
  on c7e_testflight_submissions (status, updated_at desc);

-- Backfill from the state documents (never overwrites a row that exists).
insert into c7e_testflight_submissions (
  submission_id, repo, inbox_issue_number, inbox_comment_id, inbox_comment_url,
  slack_channel_id, slack_message_ts, slack_permalink,
  status, attempts, run_id, dispatch_id, claimed_at, first_failed_at, last_failed_at,
  reason, verdict, issue_url, issue_urls, received_at, updated_at
)
select
  metadata->>'submission_id',
  coalesce(metadata->>'repo', ''),
  nullif(metadata->>'inbox_issue_number', '')::integer,
  nullif(metadata->>'inbox_comment_id', '')::bigint,
  coalesce(metadata->>'inbox_comment_url', ''),
  coalesce(metadata->>'slack_channel_id', ''),
  coalesce(metadata->>'slack_message_ts', ''),
  coalesce(metadata->>'slack_permalink', ''),
  coalesce(metadata->>'status', ''),
  coalesce(nullif(metadata->>'attempts', '')::integer, 0),
  coalesce(metadata->>'run_id', ''),
  coalesce(metadata->>'dispatch_id', ''),
  nullif(metadata->>'claimed_at', '')::timestamptz,
  nullif(metadata->>'first_failed_at', '')::timestamptz,
  case when metadata->>'status' = 'failed'
       then nullif(metadata->>'state_updated_at', '')::timestamptz end,
  coalesce(metadata->>'reason', ''),
  coalesce(metadata->>'verdict', ''),
  coalesce(metadata->>'issue_url', ''),
  coalesce(metadata->'issue_urls', '[]'::jsonb),
  coalesce(nullif(metadata->>'source_received_at', '')::timestamptz, occurred_at, now()),
  coalesce(nullif(metadata->>'state_updated_at', '')::timestamptz, updated_at, now())
from company_context_documents
where source_type = 'testflight_feedback_state'
  and metadata->>'submission_id' is not null
on conflict (submission_id) do nothing;
