CREATE TABLE IF NOT EXISTS batches (
    id               UUID PRIMARY KEY,
    idempotency_key  TEXT,
    request_hash     TEXT,
    total            INTEGER     NOT NULL CHECK (total > 0),
    correlation_id   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_batches_idempotency_key
    ON batches (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
