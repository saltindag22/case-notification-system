// Package apidocs embeds the OpenAPI specification served at /openapi.yaml.
package apidocs

import _ "embed"

// Spec is the OpenAPI 3 document.
//
//go:embed openapi.yaml
var Spec []byte
