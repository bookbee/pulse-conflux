# pulse-conflux — see specs/001-event-agent-runtime/quickstart.md
.DEFAULT_GOAL := help
BIN := bin/conflux

.PHONY: help build run test test-integration test-one lint fmt tidy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-18s\033[0m %s\n",$$1,$$2}'

build: ## Compile the service
	go build -o $(BIN) ./cmd/conflux

run: ## Run against the local .env
	go run ./cmd/conflux

test: ## Unit tests — no infrastructure required, must stay green without Docker
	go test ./...

test-integration: ## Integration tests — REQUIRES pulse-infra `full` profile: (cd ../pulse-infra && make up)
	go test -tags=integration -count=1 ./test/integration/...

test-one: ## Run a single test: make test-one NAME=TestEnvelopeParse [PKG=./internal/envelope/...]
	go test -run '$(NAME)' -v $(or $(PKG),./...)

lint: ## golangci-lint (skipped with a notice when not installed)
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed — see .golangci.yml; skipping"

fmt: ## Format and vet
	go fmt ./... && go vet ./...

tidy: ## Sync go.mod/go.sum
	go mod tidy

clean: ## Remove build artifacts
	rm -rf bin coverage.out coverage.html
