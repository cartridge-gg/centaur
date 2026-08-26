-- Let the company-context reader see hand-curated memory notes.
--
-- `centaur_cc_reader_documents_select` (0048/0050) admits only `source = 'slack'`
-- rows, so agent sessions searching company context never see documents
-- other writers put in `company_context_documents`. Memory notes
-- (`source = 'c7e_memory'`, `source_type = 'memory_note'`) are facts a user
-- explicitly asked the agent to remember, written with
-- `access_scope = 'company'`; hiding them from the agent that stored them
-- defeats the purpose.
--
-- Visibility is tied to `centaur.slack_include_public`: a principal allowed
-- to read public Slack may read company-scoped notes. A connection with no
-- access settings still sees nothing (fail closed), matching the other
-- reader policies. Other `c7e_memory` document types (rollups, metric
-- snapshots) are unchanged.
--
-- The embeddings policy delegates to this table
-- (`centaur_company_context_embedding_document_visible`), so the vector
-- search lane follows automatically.
drop policy if exists centaur_cc_reader_documents_select
    on company_context_documents;
create policy centaur_cc_reader_documents_select
    on company_context_documents
    for select
    to centaur_company_context_reader
    using (
        (
            source = 'slack'
            and metadata ->> 'channel_id' in (
                select channels.channel_id
                from slack_sync_channels channels
            )
        )
        or (
            source = 'c7e_memory'
            and source_type = 'memory_note'
            and access_scope = 'company'
            and (select centaur_company_context_include_public_slack())
        )
    );
