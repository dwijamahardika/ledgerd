.PHONY: build test test-race test-integration lint run db-up db-down docker

TEST_DATABASE_URL ?= postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable

build:
	CGO_ENABLED=0 go build -trimpath -o bin/ledgerd ./cmd/ledgerd

test:
	go test -count=1 -cover ./...

test-race:
	CGO_ENABLED=1 go test -race -count=1 ./...

## Requires a running Postgres: `make db-up` first.
test-integration:
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) go test -count=1 -tags integration ./internal/adapters/postgres/...

lint:
	golangci-lint run ./...

db-up:
	docker compose up -d db
	@until docker compose exec -T db pg_isready -U ledger -d ledger >/dev/null 2>&1; do sleep 1; done

db-down:
	docker compose down -v

run: db-up
	DATABASE_URL=$(TEST_DATABASE_URL) API_KEYS=dev-key:local-dev LOG_LEVEL=debug ENABLE_PPROF=true go run ./cmd/ledgerd

docker:
	docker compose up --build
