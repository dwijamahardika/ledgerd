# ledgerd

A production-shaped **double-entry ledger / wallet service** in Go. Small enough to read in an afternoon, built with the concerns that actually bite in production: money must never be created or lost under concurrency, retries must be safe, events must not be dropped, and a dead downstream must not take the API with it.

```
POST /v1/accounts                       create a wallet
POST /v1/accounts/{id}/deposits         treasury -> wallet
POST /v1/accounts/{id}/withdrawals      wallet -> treasury
POST /v1/transfers                      wallet -> wallet
GET  /v1/accounts/{id}                  balance + version
GET  /v1/accounts/{id}/entries          statement, keyset-paginated
GET  /v1/transfers/{id}
GET  /healthz  /readyz  /metrics  /debug/pprof/*
```

## What it demonstrates

| Concern | Solution | Where |
|---|---|---|
| Architecture | Hexagonal: `domain` (pure) ← `app` (use cases + ports) ← `adapters` (Postgres, HTTP, webhook). Composition root in `cmd/ledgerd`. | `internal/` |
| Money correctness | Integer minor units, double-entry postings, `sum(entries) == 0` invariant checked in code **and** reconciled in the integration test. Deposits/withdrawals are transfers against a per-currency treasury account, so `sum(all balances) == 0` always. | `domain/transfer.go` |
| Concurrency | `SELECT … FOR UPDATE` on both accounts **in ascending ID order** (A→B and B→A cannot deadlock) + optimistic `version` check on save + `CHECK (balance >= 0)` in the schema. Three layers, each cheap. | `app/service.go`, `postgres/account_repo.go` |
| Safe retries | `Idempotency-Key` middleware backed by Postgres: fingerprint mismatch → 422, in-flight → 409 + `Retry-After`, completed → replay with `Idempotent-Replay: true`, 5xx/panic → key released. Scoped per principal. | `httpapi/idempotency/` |
| Reliable events | Transactional outbox: the `transfer.posted` event is inserted in the same tx as the transfer. A relay claims rows with `FOR UPDATE SKIP LOCKED` (safe with N replicas), retries with capped full-jitter backoff, parks poison messages as `dead`. | `platform/outbox/`, `postgres/outbox_repo.go` |
| Downstream protection | Webhook publisher with HMAC-SHA256 signatures (+ timestamp for replay protection) behind a three-state circuit breaker. | `adapters/webhook/`, `platform/circuitbreaker/` |
| Abuse protection | Sharded token-bucket rate limiter keyed by API principal, lazy refill, janitor sweep. `X-RateLimit-*` + `Retry-After` headers. | `platform/ratelimit/` |
| Pagination | Keyset cursors on `(created_at DESC, id DESC)` with a matching composite index; opaque base64 on the wire. O(log n) at any depth, stable under inserts. | `app/cursor.go`, `postgres/ledger_repo.go` |
| Operability | `slog` JSON logs with request IDs, RED metrics + domain counters (Prometheus), pprof, liveness vs readiness probes, RFC 9457 `application/problem+json` errors carrying the request ID. | `httpapi/` |
| Lifecycle | `errgroup` + `signal.NotifyContext`: SIGTERM drains HTTP, stops the relay at a batch boundary, closes the pool. Server timeouts set. | `cmd/ledgerd/main.go` |
| Schema | Embedded forward-only SQL migrations, applied under a `pg_advisory_xact_lock` so replicas booting together don't race. | `postgres/migrate.go` |
| Testing | Table-driven domain tests; use-case tests on an in-memory store with **real rollback semantics** (fault injection proves atomicity); middleware tests with a fake clock; Postgres integration tests for deadlock-freedom, money conservation and `SKIP LOCKED`. Race detector in CI. | `*_test.go` |
| Delivery | Distroless multi-stage Dockerfile, compose stack with a webhook sink, GitHub Actions (vet, race tests, integration tests against a Postgres service, golangci-lint, image build). | root |

Dependencies are deliberately few: `pgx`, `uuid`, `prometheus/client_golang`, `x/sync`. Routing is `net/http` 1.22 patterns; no framework.

## Run it

```bash
docker compose up --build          # api :8080, postgres :5432, webhook sink :8081
```

```bash
# create + fund + transfer (see api.http for the full script)
curl -s localhost:8080/v1/accounts -H 'X-API-Key: dev-key' -H 'Idempotency-Key: a1' \
  -d '{"owner":"alice","currency":"USD"}'
```

Re-send any POST with the same `Idempotency-Key` and you get the identical response with `Idempotent-Replay: true` — and the money moves exactly once. Watch the signed event land in the sink: `docker compose logs -f webhook-sink`.

## Develop

```bash
make test               # unit tests, no external deps
make db-up              # postgres via compose
make test-integration   # concurrency / outbox / pagination against real Postgres
make run                # local server with pprof on
make lint
```

## Design notes

**Why a treasury account instead of "initial balance"?** Every movement is a balanced posting. Auditors (and reconciliation jobs) can verify `SUM(ledger_entries.amount) = 0` and `SUM(accounts.balance) = 0` at any moment. The treasury is the only account allowed negative, enforced by the DB constraint.

**Why lock in ID order and also keep a version column?** The ordered lock is what actually prevents deadlocks. The version check costs nothing and catches any future code path that updates a balance without locking first. Defence in depth.

**Why store idempotency records in Postgres, not memory?** Behind a load balancer a retry lands on a different replica. An in-process map would silently break the guarantee that matters most.

**At-least-once, not exactly-once.** The outbox guarantees an event is delivered *at least* once; consumers dedupe on `X-Ledger-Event-Id`. Exactly-once across a network boundary is a fiction; idempotent consumers are the honest contract.

**What I'd add next.** OpenTelemetry traces across HTTP → tx → webhook; a reconciliation job that alerts on balance/ledger drift; per-key idempotency TTL cleanup job; account freezes; multi-leg postings (fees) — the domain already supports N entries.

## Layout

```
cmd/ledgerd/            composition root, lifecycle
internal/domain/        entities, invariants, events (no deps)
internal/app/           use cases, ports, cursor
internal/adapters/
  postgres/             store, repos, outbox source, idempotency store, migrations
  httpapi/              handlers, middleware, problem+json, metrics
    idempotency/        Idempotency-Key middleware
  webhook/              signed publisher + verifier
internal/platform/      clock, ratelimit, circuitbreaker, backoff, outbox relay
```
