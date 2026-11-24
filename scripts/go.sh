#!/usr/bin/env sh
# Runs the go toolchain inside Docker so contributors do not need a local Go install.
# Usage: ./scripts/go.sh test ./...
set -eu
exec docker run --rm -i \
  -v "$(cd "$(dirname "$0")/.." && pwd)":/src -w /src \
  -v notification-system-gomod:/go/pkg/mod \
  -v notification-system-gocache:/root/.cache/go-build \
  -e GOFLAGS="${GOFLAGS:-}" \
  ${GO_DOCKER_ARGS:-} \
  golang:1.25 go "$@"
