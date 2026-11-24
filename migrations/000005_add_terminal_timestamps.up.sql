-- Track when a notification failed for good, and index terminal timestamps so the
-- metrics endpoint can compute windowed success/failure rates and end-to-end latency
-- without scanning the whole table.
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_notifications_sent_at ON notifications (sent_at) WHERE sent_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_notifications_failed_at ON notifications (failed_at) WHERE failed_at IS NOT NULL;
