CREATE TABLE IF NOT EXISTS provider_health (
    harness text PRIMARY KEY, exhausted_until timestamptz NOT NULL,
    last_signal_at timestamptz NOT NULL DEFAULT now(), detail text NOT NULL DEFAULT '',
    remaining_percent double precision, remaining_checked_at timestamptz
);
