DROP INDEX IF EXISTS ix_notifications_failed_at;
DROP INDEX IF EXISTS ix_notifications_sent_at;
ALTER TABLE notifications DROP COLUMN IF EXISTS failed_at;
