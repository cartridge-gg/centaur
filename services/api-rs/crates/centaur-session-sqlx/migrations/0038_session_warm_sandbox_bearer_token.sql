alter table session_warm_sandboxes
    add column if not exists bearer_token_hash text;

create index if not exists session_warm_sandboxes_bearer_token_hash_idx
    on session_warm_sandboxes (bearer_token_hash)
    where bearer_token_hash is not null;
