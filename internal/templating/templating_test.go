package templating

import (
	"errors"
	"reflect"
	"testing"
)

func TestRender(t *testing.T) {
	out, err := Render("t", "Hi {{.name}}, code {{.code}}", map[string]any{"name": "Ayşe", "code": 1234})
	if err != nil {
		t.Fatal(err)
	}
	if out != "Hi Ayşe, code 1234" {
		t.Fatalf("unexpected output %q", out)
	}
}

func TestRenderMissingVariable(t *testing.T) {
	_, err := Render("t", "Hi {{.name}}", map[string]any{})
	if !errors.Is(err, ErrRender) {
		t.Fatalf("expected ErrRender, got %v", err)
	}
}

func TestRenderSyntaxError(t *testing.T) {
	if _, err := Render("t", "Hi {{.name", nil); !errors.Is(err, ErrRender) {
		t.Fatalf("expected ErrRender, got %v", err)
	}
}

func TestParseVariables(t *testing.T) {
	_, vars, err := Parse("t", "{{.a}} {{if .b}}{{.c}}{{end}} {{.a}}")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vars, []string{"a", "b", "c"}) {
		t.Fatalf("unexpected vars %v", vars)
	}
}
