// Package migrations embeds the versioned SQL schema migrations so that the
// binaries can apply them on startup without shipping loose files.
package migrations

import "embed"

// FS holds every *.sql migration in this directory.
//
//go:embed *.sql
var FS embed.FS
