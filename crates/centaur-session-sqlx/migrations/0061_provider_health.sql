-- A harness whose model provider has no capacity left, for example an
-- exhausted subscription pool. The runtime moves sessions away from it until
-- `exhausted_until`.
create table if not exists provider_health (
    harness text primary key,
    exhausted_until timestamptz not null,
    last_signal_at timestamptz not null default now(),
    detail text not null default ''
);
