.PHONY: init-broker shutdown-broker test-unit test-integration test lint build

init-broker:
	docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10

shutdown-broker:
	docker compose down

test-unit:
	go test ./tests/unit/... -v -count=1

test-integration:
	go test ./tests/integration/... -v -count=1 -tags=integration

test: test-unit test-integration

lint:
	golangci-lint run ./...

build:
	go build ./...
