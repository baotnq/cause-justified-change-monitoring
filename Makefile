# Everything below assumes `docker compose up -d` for the integration targets.
REDIS_ADDR ?= 127.0.0.1:6379
NATS_URL   ?= nats://127.0.0.1:4222
PG_DSN     ?= postgres://audit:audit@127.0.0.1:5432/audit

.PHONY: test test-all demo bench auditd fmt vet clean

## test: the engine and its scenarios, no infrastructure required
test:
	go test ./... -count=1

## test-all: adds the Redis, NATS and Postgres integration tests
test-all:
	REDIS_ADDR=$(REDIS_ADDR) NATS_URL=$(NATS_URL) PG_DSN=$(PG_DSN) go test ./... -count=1 -race

## demo: drive the Part C scenarios against the real stack
demo:
	REDIS_ADDR=$(REDIS_ADDR) NATS_URL=$(NATS_URL) go run ./cmd/scenario

## bench: Part D, see docs/BENCHMARKS.md
bench:
	go test ./internal/bitset -bench . -benchmem -benchtime=500ms -run XXX
	REDIS_ADDR=$(REDIS_ADDR) go test ./internal/redisbits -bench . -benchtime=500ms -run XXX
	REDIS_ADDR=$(REDIS_ADDR) go test ./internal/channels -run PipelineLatency -v -count=1

## auditd: run the monitor as a service against the stack
auditd:
	REDIS_ADDR=$(REDIS_ADDR) NATS_URL=$(NATS_URL) PG_DSN=$(PG_DSN) \
	go run ./cmd/auditd -window 10s -grace 3s \
		-producers payment-service,matching-service,asset-service

fmt:
	gofmt -l -w .

vet:
	go vet ./...

clean:
	rm -f audit-report.jsonl
