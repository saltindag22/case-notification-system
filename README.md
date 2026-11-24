# Event-Driven Notification System

[![CI](https://github.com/saltindag22/case-notification-system/actions/workflows/ci.yml/badge.svg)](https://github.com/saltindag22/case-notification-system/actions/workflows/ci.yml)

A Go service that sends notifications over **SMS, e-mail and push**. It is built for burst traffic (flash sales,
breaking news): requests are accepted in milliseconds and stored durably, then processed asynchronously through
per-channel priority queues. Each channel is held to a **global** limit of 100 messages per second, failed
deliveries are retried with exponential backoff and a circuit breaker, and every status change is visible over REST,
WebSocket, metrics and distributed traces.

---

## Contents
- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Delivery & retry design](#delivery--retry-design)
- [Requirement coverage](#requirement-coverage)
- [API examples](#api-examples)
- [Observability](#observability)
- [Configuration](#configuration)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Assumptions & trade-offs](#assumptions--trade-offs)

---

## Quick start

Requirements: Docker with Compose v2. A local Go toolchain is **not** needed.

```bash
docker compose up --build        # or: docker-compose up --build
```

The first build takes about a minute. Everything (migrations included) is applied automatically; the stack is ready
when `curl localhost:8080/health/ready` returns `{"status":"ok",…}`.

| What | URL |
|---|---|
| REST API | http://localhost:8080/api/v1 |
| Swagger UI / OpenAPI | http://localhost:8080/swagger · http://localhost:8080/openapi.yaml |
| WebSocket status stream | ws://localhost:8080/ws |
| Real-time metrics (JSON) | http://localhost:8080/api/v1/metrics |
| Prometheus metrics | http://localhost:8080/metrics (api) · http://localhost:9091/metrics (worker) |
| Prometheus UI | http://localhost:9090 |
| Jaeger (traces) | http://localhost:16686 |
| RabbitMQ management | http://localhost:15672 (guest / guest) |

Send something:

```bash
curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42-otp' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Your code is 123456","priority":"high"}'
```

Run the end-to-end smoke test (14 checks against the running stack; needs `curl` and `python3`):

```bash
./scripts/smoke.sh
```

**Using webhook.site.** By default the stack talks to a bundled mock provider, so retries and the circuit breaker can be
exercised offline and without third-party rate limits. To use the real external simulator, create a webhook.site URL
that answers `202` with the documented body. You can do it in the webhook.site UI (Edit → status `202`, content type
`application/json`), or with one command:

```bash
curl -s -X POST https://webhook.site/token -H 'Content-Type: application/json' -H 'Accept: application/json' -d '{
  "default_status": 202, "default_content_type": "application/json",
  "default_content": "{\"messageId\":\"8f0c1f9e-6f6b-4c51-9d6e-2b1b8a0c7d11\",\"status\":\"accepted\",\"timestamp\":\"2026-01-01T00:00:00Z\"}"
}' | python3 -c 'import sys,json; print("https://webhook.site/" + json.load(sys.stdin)["uuid"])'
```

Then start the stack with that URL. Any 2xx counts as accepted, and `messageId` is stored as `providerMessageId`.

```bash
PROVIDER_URL=https://webhook.site/<your-uuid> docker compose up --build
```

Each delivery then shows up in the webhook.site page of that URL as
`POST {"to":"+905551234567","channel":"sms","content":"…"}`, with `Idempotency-Key`, `X-Correlation-ID` and
`traceparent` headers.

(`PROVIDER_URL_SMS`, `PROVIDER_URL_EMAIL` and `PROVIDER_URL_PUSH` can point each channel somewhere else.)

> If port 8080 is taken on your machine, use `API_PORT=8081 docker compose up --build`.

---

## Architecture

```mermaid
flowchart LR
    C[Client] -->|REST| API
    C <-->|WebSocket| API
    subgraph api [API replicas]
      API[HTTP API<br/>validation · idempotency · templates]
    end
    API -->|INSERT| PG[(PostgreSQL<br/>source of truth + backlog)]
    subgraph worker [Worker replicas]
      D[Dispatcher] -->|SKIP LOCKED<br/>priority + aging| PG
      D -->|publish w/ confirms| MQ
      MQ[[RabbitMQ<br/>notifications.sms / .email / .push<br/>x-max-priority=3]] --> P1[SMS pool]
      MQ --> P2[Email pool]
      MQ --> P3[Push pool]
      P1 & P2 & P3 -->|GCRA 100/s per channel| R[(Redis)]
      P1 & P2 & P3 -->|circuit breaker| EXT[Provider<br/>webhook.site / mock]
      P1 & P2 & P3 -->|claim / complete| PG
    end
    P1 & P2 & P3 -. status events .-> R
    R -. pub/sub .-> API
```

**Request path (synchronous, ~ms).** The API validates the request, renders the template if one is used, checks the
idempotency key and stores the notification as `pending` (or `scheduled`). It answers `202 Accepted` right away and
never calls the provider inside the request.

**Processing path (asynchronous).**
1. The **dispatcher** selects due rows per channel, highest *effective* priority first
   (`FOR UPDATE SKIP LOCKED`, which is safe with many replicas). It marks them `queued`, commits, and then publishes them
   to the channel's RabbitMQ queue with publisher confirms.
2. A **consumer pool per channel** receives each message, waits for a slot from the global rate limiter, and
   atomically **claims** the row (`queued → processing`, using one attempt). It then calls the provider and records
   the attempt together with the resulting state transition in a single transaction.

**Why PostgreSQL is the backlog and RabbitMQ is only a short buffer.** The dispatcher keeps each broker queue under
`BROKER_MAX_QUEUE_DEPTH` (2000 by default, about 20 s of work at 100/s). A flash sale of a million messages therefore
waits in PostgreSQL, where:
- priority ordering, including aging, applies to the *whole* backlog and not only to the slice already in the broker;
- scheduled sends and retry delays are just a `next_attempt_at` column, with no broker-side delay plugins or TTL tricks;
- cancelling is a single conditional `UPDATE`, because nothing has to be fished out of a queue;
- the broker never holds millions of messages, and its priority queues stay effective.

**Channel isolation.** Each channel has its own queue, consumer pool, rate-limit bucket and circuit breaker, so a slow
or failing SMS provider never delays e-mail.

### Notification lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending
    [*] --> scheduled: scheduledAt
    pending --> queued: dispatcher
    scheduled --> queued: when due
    retrying --> queued: when due
    queued --> processing: claim (attempt n)
    queued --> retrying: circuit open (no attempt used)
    processing --> sent: 2xx
    processing --> retrying: retryable error, attempts left
    processing --> failed: permanent error / attempts exhausted
    pending --> cancelled
    scheduled --> cancelled
    queued --> cancelled
    retrying --> cancelled
    failed --> pending: POST /retry (manual replay)
    sent --> [*]
    failed --> [*]
    cancelled --> [*]
```

Every transition is a conditional `UPDATE … WHERE status = …` in PostgreSQL, so concurrent actors such as a cancel
request, duplicate broker deliveries or the reaper cannot corrupt state.

---

## Delivery & retry design

The assignment leaves this section to the candidate. These are the decisions and the reasons behind them.

| Concern | Decision |
|---|---|
| **Delivery guarantee** | *At-least-once* towards the provider, with idempotency at every hop. Exactly-once is not achievable over HTTP. Each provider request carries `Idempotency-Key: <notification id>` (the same value on every attempt), so a provider that deduplicates (the mock does) never sends twice. |
| **Error classification** | Retryable: timeouts, connection errors, `408`, `425`, `429`, `5xx`. Permanent: every other `4xx`, because the request itself is wrong and retrying cannot help. Permanent errors fail immediately and do not count against the circuit breaker. |
| **Backoff** | Exponential with *equal jitter*: `delay = d/2 + rand(d/2)`, where `d = 2s · 2^(attempt-1)`, capped at 10 min. Defaults are 5 attempts, i.e. retries after ~2s, 4s, 8s and 16s. Jitter matters during bursts: without it, thousands of messages that failed together would retry together. |
| **Retry-After** | A provider's `Retry-After` header (seconds or HTTP date) overrides the computed delay, capped at the maximum delay. |
| **Where retries wait** | In PostgreSQL (`status = retrying`, `next_attempt_at`), not in a sleeping goroutine or a broker TTL queue. Workers stay free, and a worker restart loses nothing. |
| **Dead letters** | Exhausted or permanent failures end in `failed` with `last_error` and the full attempt history (`attemptHistory`). `POST /notifications/{id}/retry` replays one with a fresh attempt budget. Undecodable broker messages (poison messages) go to the `notifications.dead` queue. |
| **Circuit breaker** | One per channel. After 5 consecutive provider failures it opens for 30 s, then allows a single half-open probe. While it is open, messages are *deferred* back to the backlog **without using an attempt**, so an outage does not burn through every notification's retry budget. |
| **Rate limiting** | Redis GCRA in *reservation* mode, shared by all worker replicas and timed by Redis' own clock. Each send reserves the next 10 ms slot, which spaces sends evenly with at most 100 in any 1-second window. The reservation needs one round trip, never retries, and causes no thundering herd. |
| **Priority** | `high`, `normal` and `low` map to effective priorities 3, 2 and 1. The dispatcher orders the backlog by effective priority, and RabbitMQ priority queues with a small prefetch keep the order inside the buffer. **Aging** raises a waiting message one level every `PRIORITY_AGING_INTERVAL` (5 min), so low priority traffic cannot starve behind an endless stream of high priority messages. |
| **Crash safety** | The reaper returns rows stuck in `queued` (message lost) or `processing` (worker died mid-attempt) to the backlog. Duplicates this may cause are harmless: a claim requires a matching `(status, attempts)` pair, so stale broker messages are skipped. |
| **Graceful shutdown** | On SIGTERM the worker stops consuming but finishes every attempt it has already claimed before it closes channels. A message whose rate-limiter wait is interrupted goes back to the queue untouched. |

---

## Requirement coverage

| Requirement | Where / how |
|---|---|
| Create notification (recipient, channel, content, priority) | `POST /api/v1/notifications` |
| Batch up to 1000 | `POST /api/v1/notifications/batch`: validated all-or-nothing with indexed field errors, inserted in one pipelined transaction |
| Query by id / batch id | `GET /notifications/{id}` (with attempt history) · `GET /batches/{id}` (counts per status) · `GET /batches/{id}/notifications` |
| Cancel pending | `POST /notifications/{id}/cancel` · `POST /batches/{id}/cancel`, both race-free conditional updates (`409` once processing started) |
| List with filters + pagination | `GET /notifications?status=&channel=&batchId=&from=&to=&limit=&cursor=`: keyset pagination on time-ordered UUIDv7 ids |
| Async queue workers | Dispatcher + RabbitMQ + per-channel consumer pools (`internal/worker`) |
| 100 msg/s per channel | Global Redis GCRA limiter (`internal/ratelimit`); a load test of 1000 SMS ran at a steady ~100/s |
| Priority queue | Effective priority ordering in PostgreSQL + RabbitMQ `x-max-priority` + aging |
| Content validation | `internal/domain/validation.go`: E.164 numbers, RFC 5322 addresses, device tokens, GSM-7/UCS-2 aware SMS segment limits, e-mail subject rules (incl. header injection), push payload size |
| Idempotency | `Idempotency-Key` header (or `idempotencyKey` per item): unique index + request fingerprint (reusing a key with a different payload → `409`); whole-batch idempotency; idempotent consumers; provider idempotency key |
| Metrics endpoint | `GET /api/v1/metrics` (queue depth, success/failure rates, provider and end-to-end latency p50/p95/p99) + Prometheus `/metrics` |
| Structured logging + correlation IDs | `log/slog` JSON; `X-Correlation-ID` accepted or generated, stored with the notification, carried through AMQP headers to the worker and the provider request; trace/span ids in every log line |
| Health check | `/health/live` and `/health/ready` (dependency checks) on both api and worker |
| **Bonus:** failure handling | Retries, backoff, Retry-After, circuit breaker, dead-letter state + replay, reaper, graceful shutdown |
| **Bonus:** scheduled notifications | `scheduledAt` (up to 30 days ahead) |
| **Bonus:** templates | `/api/v1/templates` CRUD; `templateId` + `variables` with strict rendering |
| **Bonus:** WebSocket | `GET /ws` with per-client filters, fed by Redis Pub/Sub so it works with many API replicas |
| **Bonus:** distributed tracing | OpenTelemetry → Jaeger; one trace spans HTTP request → dispatch → consume → provider call |
| **Bonus:** CI/CD | GitHub Actions: golangci-lint, OpenAPI lint, race-enabled unit + integration tests with Postgres/Redis services, Docker build |
| Deliverables | Docker Compose one-command setup · OpenAPI spec · versioned migrations (`migrations/`) · `make test` |

---

## API examples

Full reference: **[Swagger UI](http://localhost:8080/swagger)**, or the spec at [`api/openapi.yaml`](api/openapi.yaml).

<details>
<summary><b>Create (SMS, e-mail, push, scheduled)</b></summary>

```bash
# e-mail (subject required)
curl -s -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' -d '{
  "recipient": "user@example.com", "channel": "email",
  "subject": "Your order shipped", "content": "<p>It is on the way!</p>",
  "metadata": {"orderId": "A-1001"}
}'

# push, scheduled for later
curl -s -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' -d '{
  "recipient": "fcm-token_ABCDEFGHIJKLMNOP", "channel": "push",
  "subject": "Flash sale", "content": "Starts in 5 minutes", "priority": "high",
  "scheduledAt": "2026-12-01T09:00:00Z"
}'
```

`202 Accepted`:
```json
{
  "id": "01a208a9-c554-7e6b-9841-aff7c412bbf6",
  "correlationId": "demo-corr-1",
  "channel": "sms",
  "recipient": "+905551234567",
  "content": "Your code is 123456",
  "priority": "high",
  "status": "pending",
  "attempts": 0,
  "maxAttempts": 5,
  "nextAttemptAt": "2026-11-23T13:47:53.55694Z",
  "createdAt": "2026-11-23T13:47:53.55694Z",
  "updatedAt": "2026-11-23T13:47:53.55694Z"
}
```
A replay with the same `Idempotency-Key` returns `200` and `Idempotent-Replayed: true`. Reusing the key with a
different body returns `409 idempotency_conflict`.
</details>

<details>
<summary><b>Validation errors</b></summary>

```bash
curl -s -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"0555","channel":"sms","content":"","priority":"urgent"}'
```
```json
{
  "error": {
    "code": "validation_error",
    "message": "request validation failed",
    "fields": [
      {"field": "priority", "message": "unsupported priority \"urgent\" (allowed: high, normal, low)"},
      {"field": "content", "message": "is required"},
      {"field": "recipient", "message": "must be a phone number in E.164 format, e.g. +905551234567"}
    ]
  },
  "correlationId": "aac087ae-0370-4e07-80db-2dccbbfe426f"
}
```
</details>

<details>
<summary><b>Batch</b></summary>

```bash
curl -s -X POST localhost:8080/api/v1/notifications/batch \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: campaign-2026-11-flash' -d '{
  "notifications": [
    {"recipient": "+905551234567", "channel": "sms", "content": "Flash sale!", "priority": "high"},
    {"recipient": "user@example.com", "channel": "email", "subject": "Flash sale", "content": "50% off"}
  ]
}'
# → {"batchId":"…","total":2,"created":2,"replayed":false,"notifications":[{"index":0,"notification":{…}}, …]}

curl -s localhost:8080/api/v1/batches/<batchId>
# → {"batchId":"…","total":2,"counts":{"sent":2},"completed":true,"createdAt":"…"}
```
If any item is invalid, nothing is stored and errors are reported per index (`notifications[3].recipient`).
</details>

<details>
<summary><b>Templates</b></summary>

```bash
curl -s -X POST localhost:8080/api/v1/templates -H 'Content-Type: application/json' -d '{
  "name": "welcome-email", "channel": "email",
  "subject": "Welcome {{.name}}", "content": "Hi {{.name}}, here is {{.discount}}% off."
}'

curl -s -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' -d '{
  "recipient": "ayse@example.com", "templateId": "<id>",
  "variables": {"name": "Ayşe", "discount": 20}
}'
# subject "Welcome Ayşe", content "Hi Ayşe, here is 20% off." A missing variable is a 400.
```
</details>

<details>
<summary><b>Query, list, cancel, retry</b></summary>

```bash
curl -s localhost:8080/api/v1/notifications/<id>          # includes attemptHistory
curl -s 'localhost:8080/api/v1/notifications?status=failed&channel=sms&from=2026-11-01T00:00:00Z&limit=50'
curl -s 'localhost:8080/api/v1/notifications?cursor=<pagination.nextCursor>'
curl -s -X POST localhost:8080/api/v1/notifications/<id>/cancel   # 409 if already processing/sent
curl -s -X POST localhost:8080/api/v1/notifications/<id>/retry    # failed → pending
```
</details>

<details>
<summary><b>WebSocket</b></summary>

```bash
# any WebSocket client, e.g. `websocat`
websocat 'ws://localhost:8080/ws?batchId=<batchId>'
```
```json
{"type":"connected","message":"subscribed to notification status events"}
{"type":"notification.status","notificationId":"…","batchId":"…","channel":"sms","status":"queued","attempts":0,"timestamp":"…"}
{"type":"notification.status","notificationId":"…","batchId":"…","channel":"sms","status":"processing","attempts":1,"timestamp":"…"}
{"type":"notification.status","notificationId":"…","batchId":"…","channel":"sms","status":"sent","attempts":1,"timestamp":"…"}
```
Subscribe to more ids at any time by sending `{"action":"subscribe","notificationIds":["…"],"batchIds":["…"]}`.
Without filters every event is delivered, which suits dashboards.
</details>

---

## Observability

- **Real-time metrics:** `GET /api/v1/metrics?window=5m` returns queue depth per channel and status, attempt-level and
  notification-level success rates, and provider and end-to-end latency percentiles. It is computed from PostgreSQL, so
  it stays correct with any number of replicas.
- **Prometheus:** HTTP rate and latency per route, notifications created, dispatched and delivered by result, provider
  latency histograms, end-to-end latency, rate-limiter wait time, circuit breaker state, broker queue depth (including
  the dead-letter queue), in-flight work and backlog by status. Prometheus in the compose stack scrapes all services.
- **Logs:** JSON (`log/slog`). Every line carries `service`, `correlation_id`, `trace_id` and `span_id`, for example:
  ```json
  {"time":"…","level":"INFO","msg":"notification sent","service":"notification-worker","notification_id":"…","channel":"sms","attempt":1,"provider_message_id":"…","duration_ms":42,"correlation_id":"demo-corr-1","trace_id":"4bf9…","span_id":"00f0…"}
  ```
- **Tracing:** OpenTelemetry with W3C trace context. The `traceparent` is stored with the notification, because the
  API-to-worker hop is asynchronous and may happen hours later for scheduled sends. The dispatcher then continues the
  original trace into AMQP headers, and the HTTP call to the provider propagates it further. You can follow a single
  request end to end in Jaeger.
- **Health:** `/health/live` (process) and `/health/ready` (PostgreSQL, Redis and, for the worker, RabbitMQ). The
  distroless images run `<binary> healthcheck` for the Docker HEALTHCHECK.

---

## Configuration

All settings are environment variables with defaults that work out of the box.

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDR` | `:8080` api · `:9091` worker | Listen address |
| `DATABASE_URL` | `postgres://notifications:notifications@localhost:5432/notifications?sslmode=disable` | PostgreSQL |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis (rate limiter, status events) |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ |
| `RUN_MIGRATIONS` | `true` | Apply migrations on start-up (advisory-locked, safe with replicas) |
| `PROVIDER_URL` | `http://localhost:8090/webhook` | Provider endpoint (webhook.site URL) |
| `PROVIDER_URL_SMS` / `_EMAIL` / `_PUSH` | – | Per-channel override |
| `PROVIDER_TIMEOUT` | `5s` | Per request timeout |
| `RATE_LIMIT_PER_SECOND` | `100` | Per channel, global across workers |
| `RATE_LIMIT_BURST` | `1` | `1` = strict pacing (never more than the rate in any 1 s window) |
| `WORKER_CONCURRENCY` | `20` | Consumers per channel (also the AMQP prefetch) |
| `MAX_ATTEMPTS` | `5` | Total attempts including the first |
| `RETRY_BASE_DELAY` / `RETRY_MAX_DELAY` | `2s` / `10m` | Exponential backoff bounds |
| `BREAKER_FAILURE_THRESHOLD` / `BREAKER_OPEN_TIMEOUT` | `5` / `30s` | Circuit breaker |
| `DISPATCH_INTERVAL` / `DISPATCH_BATCH_SIZE` | `250ms` / `500` | Dispatcher loop |
| `BROKER_MAX_QUEUE_DEPTH` | `2000` | Per channel broker buffer; the rest waits in PostgreSQL |
| `PRIORITY_AGING_INTERVAL` | `5m` | Promote waiting messages one level per interval |
| `STUCK_QUEUED_AFTER` / `STUCK_PROCESSING_AFTER` | `5m` / `2m` | Reaper thresholds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | – | OTLP/HTTP collector, e.g. `http://jaeger:4318` (empty: no export) |
| `OTEL_TRACES_SAMPLER_RATIO` | `1.0` | Trace sampling ratio |
| `WS_ALLOWED_ORIGINS` | – (any) | Comma separated allowed WebSocket origins |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `MOCK_FAILURE_RATE` / `MOCK_THROTTLE_RATE` / `MOCK_LATENCY_MS` | `0.05` / `0.01` / `30` | Mock provider behaviour |

Try the circuit breaker: `MOCK_FAILURE_RATE=1 docker compose up -d mock-provider`, send a few messages and watch
`notification_worker_circuit_breaker_state` and the worker logs.

---

## Testing

```bash
make test        # full suite in Docker: unit + integration against real PostgreSQL and Redis (race detector on)
make test-unit   # unit tests only; integration tests skip without DATABASE_URL / REDIS_URL
make lint        # golangci-lint (same version as CI)
./scripts/smoke.sh   # end-to-end against a running stack
```

`make test` is `docker compose run --rm tests`, which needs no Go installation.

What is covered:
- **Domain:** SMS encoding and segment maths (GSM-7 vs UCS-2, extended characters, emoji), every channel's validation
  rules, state rules.
- **Retry / circuit breaker / rate limiter:** backoff growth, caps, jitter bounds, Retry-After, breaker state machine
  with a fake clock, global limiter shared by two "replicas" through real Redis.
- **Repository (integration, real PostgreSQL):** idempotency (including 20 concurrent inserts with the same key),
  batch replay, cancel rules, dispatch ordering, duplicate claim protection, stale attempt protection, retry and fail
  flow, reaper, priority aging, keyset pagination, commit-before-publish regression.
- **Worker:** success, retry, Retry-After, exhausted attempts, permanent errors not tripping the breaker, deferral
  without using an attempt, stale message skipping, shutdown during the limiter wait, dispatcher depth limits and
  publish failure.
- **API:** status codes and error envelopes, idempotent replay headers, body limits, filter and cursor parsing,
  metrics, health, correlation ids, WebSocket filtering and dynamic subscription.

---

## Project layout

```
cmd/
  api/            REST API, WebSocket, metrics, health (applies migrations)
  worker/         dispatcher + consumer pools + ops endpoints
  mockprovider/   local stand-in for webhook.site (failures, throttling, idempotency)
api/              OpenAPI 3 spec (embedded, served at /openapi.yaml)
migrations/       versioned SQL migrations (golang-migrate, embedded)
internal/
  domain/         model, state rules, validation (no infrastructure deps)
  service/        use cases: create/batch/cancel/retry, templates, fingerprinting
  repository/     PostgreSQL: idempotent inserts, conditional transitions, dispatch, stats
  worker/         dispatcher, processor, consumer pool
  queue/          RabbitMQ topology, confirms, consumers
  provider/       HTTP client + error classification
  ratelimit/      Redis GCRA (global) + in-memory limiter
  retry/          backoff policy
  circuitbreaker/ per-channel breaker
  templating/     strict text/template rendering
  events/         Redis Pub/Sub status events
  api/            handlers, middleware, WebSocket hub
  metrics/ tracing/ logging/ config/ healthcheck/
deploy/           Prometheus and Postgres init config
scripts/          smoke test, dockerised go wrapper
```

---

## Assumptions & trade-offs

- **Batch semantics:** validation is all-or-nothing, so a client never has to reconcile a half-accepted campaign. An
  item whose own `idempotencyKey` already exists is returned as `duplicate: true` and is not created twice.
- **SMS length:** concatenated messages of up to 10 segments are allowed. A single Turkish letter (ş, ğ, ı, İ, ç)
  switches the whole message to UCS-2 (70/67 characters per segment), and the API accounts for that.
- **E-mail:** `subject` is required. Content is treated as an opaque body (HTML or text) of up to 100 KB.
- **Cancel:** possible until the worker claims the notification. After that it is `409`, because the provider call
  may already be in flight.
- **Scheduling horizon:** 30 days, which keeps the backlog bounded and catches typos in dates.
- **Rate limit scope:** 100/s per channel across *all* worker replicas. Per-provider or per-tenant limits would be the
  same limiter with a different key.
- **Starvation vs. strict priority:** aging trades strict priority for fairness. A low priority message waits at most
  about two aging intervals before it competes as high.
- **Crash-only worker:** if the RabbitMQ connection drops, the worker exits and is restarted by Compose (`restart:
  unless-stopped`) instead of carrying reconnect state machines. Nothing is lost, because state lives in PostgreSQL.
- **Out of scope / next steps:** authentication and multi-tenancy (the API is expected behind a gateway), real provider
  adapters (Twilio, SES, FCM) behind the existing `provider.Sender` interface, delivery receipts via provider
  callbacks, archiving or partitioning of old notifications, a Grafana dashboard, and Kubernetes manifests with an HPA
  driven by `notification_backlog`.
