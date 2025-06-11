.PHONY: test test-race test-cover lint fmt vet build clean example help

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run tests
	go test -v ./...

test-race: ## Run tests with race detection
	go test -v -race ./...

test-cover: ## Run tests with coverage
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint: ## Run linter
	golangci-lint run

fmt: ## Format code
	go fmt ./...

vet: ## Run go vet
	go vet ./...

build: ## Build the library
	go build ./...

example: ## Run the example
	cd example && go run main.go

clean: ## Clean build artifacts
	go clean ./...
	rm -f coverage.out coverage.html

check: fmt vet lint test ## Run all checks (format, vet, lint, test)

ci: check test-race ## Run CI pipeline locally
