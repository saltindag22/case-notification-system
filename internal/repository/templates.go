package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

const templateColumns = `id, name, channel, subject, content, created_at, updated_at`

func scanTemplate(row pgx.Row) (*domain.Template, error) {
	var t domain.Template
	var ch string
	if err := row.Scan(&t.ID, &t.Name, &ch, &t.Subject, &t.Content, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	t.Channel = domain.Channel(ch)
	return &t, nil
}

// CreateTemplate inserts a template; names are unique.
func (s *Store) CreateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error) {
	out, err := scanTemplate(s.pool.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, subject, content, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6) RETURNING `+templateColumns,
		t.ID, t.Name, string(t.Channel), t.Subject, t.Content, t.CreatedAt))
	if isUniqueViolation(err) {
		return nil, domain.ErrConflict
	}
	return out, err
}

// GetTemplate loads a template by id.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx, `SELECT `+templateColumns+` FROM templates WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return t, err
}

// ListTemplates returns all templates ordered by name.
func (s *Store) ListTemplates(ctx context.Context) ([]*domain.Template, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+templateColumns+` FROM templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*domain.Template{}
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpdateTemplate replaces name, channel, subject and content.
func (s *Store) UpdateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error) {
	out, err := scanTemplate(s.pool.QueryRow(ctx, `
		UPDATE templates SET name = $2, channel = $3, subject = $4, content = $5, updated_at = now()
		WHERE id = $1 RETURNING `+templateColumns,
		t.ID, t.Name, string(t.Channel), t.Subject, t.Content))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, domain.ErrNotFound
	case isUniqueViolation(err):
		return nil, domain.ErrConflict
	}
	return out, err
}

// DeleteTemplate removes a template. Notifications keep their rendered content.
func (s *Store) DeleteTemplate(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM templates WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}
