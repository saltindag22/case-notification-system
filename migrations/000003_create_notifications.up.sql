-- Notifications are the source of truth for delivery state. The table also acts as
-- the backlog: the dispatcher moves due rows (pending/scheduled/retrying) into the
-- message broker in small chunks, so ordering by priority happens here.
CREATE TABLE IF NOT EXISTS notifications (
    id                   UUID PRIMARY KEY,               -- UUIDv7: time ordered, used as pagination cursor
    batch_id             UUID REFERENCES batches (id) ON DELETE SET NULL,
    idempotency_key      TEXT,
    request_hash         TEXT,
    correlation_id       TEXT,
    trace_parent         TEXT,                           -- W3C traceparent captured at creation
    channel              TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient            TEXT        NOT NULL,
    subject              TEXT,
    content              TEXT        NOT NULL,
    template_id          UUID REFERENCES templates (id) ON DELETE SET NULL,
    metadata             JSONB,
    priority             SMALLINT    NOT NULL CHECK (priority BETWEEN 1 AND 3),          -- 1 low, 2 normal, 3 high
    effective_priority   SMALLINT    NOT NULL CHECK (effective_priority BETWEEN 1 AND 3), -- priority after aging
    status               TEXT        NOT NULL CHECK (status IN
                             ('pending', 'scheduled', 'queued', 'processing', 'retrying', 'sent', 'failed', 'cancelled')),
    attempts             INTEGER     NOT NULL DEFAULT 0,
    max_attempts         INTEGER     NOT NULL CHECK (max_attempts > 0),
    scheduled_at         TIMESTAMPTZ,
    next_attempt_at      TIMESTAMPTZ NOT NULL,
    provider_message_id  TEXT,
    last_error           TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at              TIMESTAMPTZ,
    cancelled_at         TIMESTAMPTZ
);

-- Idempotency: one notification per client supplied key.
CREATE UNIQUE INDEX IF NOT EXISTS ux_notifications_idempotency_key
    ON notifications (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Dispatcher: due work per channel, highest (aged) priority first.
CREATE INDEX IF NOT EXISTS ix_notifications_dispatch
    ON notifications (channel, effective_priority DESC, next_attempt_at)
    WHERE status IN ('pending', 'scheduled', 'retrying');

-- Reaper: rows stuck in transient states.
CREATE INDEX IF NOT EXISTS ix_notifications_in_flight
    ON notifications (updated_at)
    WHERE status IN ('queued', 'processing');

-- Listing / filtering (keyset pagination on id DESC).
CREATE INDEX IF NOT EXISTS ix_notifications_status_channel_id ON notifications (status, channel, id DESC);
CREATE INDEX IF NOT EXISTS ix_notifications_channel_id ON notifications (channel, id DESC);
CREATE INDEX IF NOT EXISTS ix_notifications_created_at ON notifications (created_at);
CREATE INDEX IF NOT EXISTS ix_notifications_batch_id ON notifications (batch_id) WHERE batch_id IS NOT NULL;
