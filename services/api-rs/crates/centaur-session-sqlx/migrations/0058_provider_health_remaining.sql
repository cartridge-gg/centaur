-- The remaining usage of the model provider of a harness, in percent (0 to
-- 100), from the last provider health check, and when that check ran. The
-- runtime uses a recent value to choose between failover candidates that are
-- not exhausted. Null: the check did not report it.
alter table provider_health
    add column if not exists remaining_percent double precision,
    add column if not exists remaining_checked_at timestamptz;
