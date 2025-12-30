.PHONY: test test-race bench lint lint-fix coverage clean install-tools install-hooks all help check ci

# Default target
all: test lint

# Display help
help:
	@echo "DOCQL Development Commands"
	@echo "=========================="
	@echo ""
	@echo "Testing & Quality:"
	@echo "  make test             - Run unit tests (fast)"
	@echo "  make test-race        - Run unit tests with race detector"
	@echo "  make bench            - Run benchmarks"
	@echo "  make lint             - Run golangci-lint"
	@echo "  make lint-fix         - Run golangci-lint with auto-fix"
	@echo "  make coverage         - Generate coverage report"
	@echo "  make check            - Run lint + tests with race detector (pre-commit)"
	@echo "  make ci               - Full CI simulation locally"
	@echo ""
	@echo "Other:"
	@echo "  make install-tools    - Install required development tools"
	@echo "  make install-hooks    - Install git pre-commit hook"
	@echo "  make clean            - Clean generated files"
	@echo "  make all              - Run tests and lint (default)"

# Run unit tests only
test:
	@echo "Running unit tests..."
	@go test -v -short ./...

# Run unit tests with race detector
test-race:
	@echo "Running unit tests with race detector..."
	@go test -v -race -short ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=1s -short ./...

# Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run --config=.golangci.yml --timeout=5m

# Run linters with auto-fix
lint-fix:
	@echo "Running linters with auto-fix..."
	@golangci-lint run --config=.golangci.yml --fix

# Generate coverage report
coverage:
	@echo "Generating coverage report..."
	@go test -short -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report generated: coverage.html"

# Clean generated files
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete
	@go clean -cache

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

# Install git pre-commit hook
install-hooks:
	@echo "Installing git hooks..."
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make check' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed!"

# Quick check - run tests with race detector and lint (pre-commit)
check: lint test-race
	@echo "All checks passed!"

# CI simulation - full CI pipeline locally
ci: clean lint test-race coverage bench
	@echo "CI simulation complete!"
