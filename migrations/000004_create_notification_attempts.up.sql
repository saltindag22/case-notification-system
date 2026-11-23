-- One row per delivery attempt: the audit trail behind retries and latency metrics.
CREATE TABLE IF NOT EXISTS notification_attempts (
    id                   BIGSERIAL PRIMARY KEY,
    notification_id      UUID        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_number       INTEGER     NOT NULL,
    outcome              TEXT        NOT NULL CHECK (outcome IN ('success', 'retryable_error', 'permanent_error')),
    http_status          INTEGER,
    error                TEXT,
    provider_message_id  TEXT,
    duration_ms          INTEGER     NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_notification_attempts_notification_id ON notification_attempts (notification_id, attempt_number);
CREATE INDEX IF NOT EXISTS ix_notification_attempts_created_at ON notification_attempts (created_at);
