CREATE TABLE IF NOT EXISTS templates (
    id          UUID PRIMARY KEY,
    name        TEXT        NOT NULL,
    channel     TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    subject     TEXT,
    content     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_templates_name ON templates (name);
