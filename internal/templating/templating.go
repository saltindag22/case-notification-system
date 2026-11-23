// Package templating renders message templates with variable substitution.
//
// Templates use Go text/template syntax, e.g. "Hi {{.firstName}}, your code is {{.code}}".
// Rendering is strict: a variable referenced by the template but missing from the
// supplied variables is an error, so half-rendered messages are never sent.
package templating

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"text/template/parse"
)

// MaxTemplateBytes bounds the size of a stored template body.
const MaxTemplateBytes = 100_000

// ErrRender is returned (wrapped) for any parse or execution failure.
var ErrRender = errors.New("template render failed")

// Parse compiles a template body and returns the variable names it references.
func Parse(name, body string) (*template.Template, []string, error) {
	if len(body) > MaxTemplateBytes {
		return nil, nil, fmt.Errorf("%w: template exceeds %d bytes", ErrRender, MaxTemplateBytes)
	}
	t, err := template.New(name).Option("missingkey=error").Parse(body)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrRender, err)
	}
	return t, variables(t), nil
}

// Render parses and executes body with vars.
func Render(name, body string, vars map[string]any) (string, error) {
	t, _, err := Parse(name, body)
	if err != nil {
		return "", err
	}
	if vars == nil {
		vars = map[string]any{}
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("%w: %v", ErrRender, cleanExecError(err))
	}
	return buf.String(), nil
}

// variables walks the parse tree and collects top-level field names ({{.name}}).
func variables(t *template.Template) []string {
	seen := map[string]struct{}{}
	var out []string
	var walk func(n parse.Node)
	walk = func(n parse.Node) {
		if n == nil {
			return
		}
		switch x := n.(type) {
		case *parse.ListNode:
			if x == nil {
				return
			}
			for _, c := range x.Nodes {
				walk(c)
			}
		case *parse.ActionNode:
			walk(x.Pipe)
		case *parse.PipeNode:
			if x == nil {
				return
			}
			for _, c := range x.Cmds {
				walk(c)
			}
		case *parse.CommandNode:
			for _, a := range x.Args {
				walk(a)
			}
		case *parse.FieldNode:
			if len(x.Ident) > 0 {
				if _, ok := seen[x.Ident[0]]; !ok {
					seen[x.Ident[0]] = struct{}{}
					out = append(out, x.Ident[0])
				}
			}
		case *parse.IfNode:
			walk(x.Pipe)
			walk(x.List)
			walk(x.ElseList)
		case *parse.RangeNode:
			walk(x.Pipe)
			walk(x.List)
			walk(x.ElseList)
		case *parse.WithNode:
			walk(x.Pipe)
			walk(x.List)
			walk(x.ElseList)
		}
	}
	if t.Tree != nil {
		walk(t.Root)
	}
	return out
}

// cleanExecError strips the internal template position prefix for nicer API errors.
func cleanExecError(err error) string {
	msg := err.Error()
	if i := strings.LastIndex(msg, ": "); i >= 0 && strings.Contains(msg, "map has no entry for key") {
		return "missing variable" + msg[i:]
	}
	return msg
}
