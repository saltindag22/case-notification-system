package service

import (
	"context"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/templating"
)

var templateNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.\-]{0,99}$`)

// TemplateInput is the API payload for creating or replacing a template.
type TemplateInput struct {
	Name    string  `json:"name"`
	Channel string  `json:"channel"`
	Subject *string `json:"subject,omitempty"`
	Content string  `json:"content"`
}

func (s *Service) buildTemplate(in TemplateInput) (*domain.Template, error) {
	v := &domain.ValidationError{}
	t := &domain.Template{Name: strings.TrimSpace(in.Name), Content: in.Content}
	if !templateNamePattern.MatchString(t.Name) {
		v.Add("name", "must be 1-100 chars of lowercase letters, digits, '_', '-', '.'")
	}
	ch, err := domain.ParseChannel(in.Channel)
	if err != nil {
		v.Add("channel", "%s", err.Error())
	}
	t.Channel = ch
	if strings.TrimSpace(in.Content) == "" {
		v.Add("content", "is required")
	} else if _, _, err := templating.Parse(t.Name, in.Content); err != nil {
		v.Add("content", "%s", err.Error())
	}
	if in.Subject != nil {
		subj := strings.TrimSpace(*in.Subject)
		if subj != "" {
			if _, _, err := templating.Parse(t.Name+".subject", subj); err != nil {
				v.Add("subject", "%s", err.Error())
			}
			t.Subject = &subj
		}
	}
	switch ch {
	case domain.ChannelEmail:
		if t.Subject == nil {
			v.Add("subject", "is required for email templates")
		}
	case domain.ChannelSMS:
		if t.Subject != nil {
			v.Add("subject", "is not supported for sms")
		}
	}
	return t, v.OrNil()
}

// CreateTemplate validates and stores a template.
func (s *Service) CreateTemplate(ctx context.Context, in TemplateInput) (*domain.Template, error) {
	t, err := s.buildTemplate(in)
	if err != nil {
		return nil, err
	}
	t.ID = domain.NewID()
	t.CreatedAt = s.now().UTC()
	return s.store.CreateTemplate(ctx, t)
}

// UpdateTemplate replaces a template.
func (s *Service) UpdateTemplate(ctx context.Context, id uuid.UUID, in TemplateInput) (*domain.Template, error) {
	t, err := s.buildTemplate(in)
	if err != nil {
		return nil, err
	}
	t.ID = id
	return s.store.UpdateTemplate(ctx, t)
}

// GetTemplate returns a template.
func (s *Service) GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	return s.store.GetTemplate(ctx, id)
}

// ListTemplates returns all templates.
func (s *Service) ListTemplates(ctx context.Context) ([]*domain.Template, error) {
	return s.store.ListTemplates(ctx)
}

// DeleteTemplate deletes a template.
func (s *Service) DeleteTemplate(ctx context.Context, id uuid.UUID) error {
	return s.store.DeleteTemplate(ctx, id)
}
