# Every target works without a local Go installation: Go commands fall back to
# a golang container when `go` is not on PATH.
GO := $(shell command -v go >/dev/null 2>&1 && echo go || echo ./scripts/go.sh)
COMPOSE := docker compose
LINT_IMAGE := golangci/golangci-lint:v2.15.0

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start the whole stack (api, worker, postgres, redis, rabbitmq, jaeger, prometheus, mock provider)
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Follow api and worker logs
	$(COMPOSE) logs -f api worker

.PHONY: test
test: ## Run the full test suite (unit + integration against real Postgres/Redis) in Docker
	$(COMPOSE) run --rm tests

.PHONY: test-unit
test-unit: ## Run unit tests only (integration tests skip without DATABASE_URL/REDIS_URL)
	$(GO) test -race -count=1 ./...

.PHONY: cover
cover: test ## Show coverage summary of the last full test run
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	docker run --rm -v "$(CURDIR)":/src -w /src $(LINT_IMAGE) golangci-lint run ./...

.PHONY: lint-openapi
lint-openapi: ## Validate the OpenAPI specification
	docker run --rm -v "$(CURDIR)":/spec -w /spec node:22-alpine npx --yes @redocly/cli@1.34.5 lint api/openapi.yaml

.PHONY: fmt
fmt: ## Format code
	$(GO) fmt ./...

.PHONY: build
build: ## Build all binaries into ./bin
	CGO_ENABLED=0 $(GO) build -o bin/ ./cmd/...

.PHONY: smoke
smoke: ## Run the end-to-end smoke test in Docker (starts the stack if needed)
	$(COMPOSE) run --rm smoke
