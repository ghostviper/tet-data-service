# TET Data Service Makefile

.PHONY: build run test clean docker-build docker-run docker-stop deps

# Variables
APP_NAME=tet-data-service
DOCKER_IMAGE=tet-data-service:latest

# Build the application
build:
	@echo "Building $(APP_NAME)..."
	go build -o bin/$(APP_NAME) ./cmd/server

# Run the application locally
run: build
	@echo "Running $(APP_NAME)..."
	./bin/$(APP_NAME)

# Run with custom parameters
run-dev: build
	@echo "Running $(APP_NAME) in development mode..."
	./bin/$(APP_NAME) --config .env --log-level debug --interval 60

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

# Test the application
test:
	@echo "Running tests..."
	go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	go clean

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE) .

# Docker run with compose
docker-run:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d

# Docker run in foreground
docker-run-fg:
	@echo "Starting services with Docker Compose (foreground)..."
	docker-compose up

# Stop Docker services
docker-stop:
	@echo "Stopping Docker services..."
	docker-compose down

# View logs
docker-logs:
	@echo "Showing Docker logs..."
	docker-compose logs -f tet-data-service

# Setup environment file
setup-env:
	@echo "Setting up environment file..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Please edit .env file with your configuration"; \
	else \
		echo ".env file already exists"; \
	fi

# Format code
fmt:
	@echo "Formatting Go code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting Go code..."
	golangci-lint run

# Install development tools
dev-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Full development setup
dev-setup: deps dev-tools setup-env
	@echo "Development environment setup complete!"
	@echo "Don't forget to:"
	@echo "1. Edit .env file with your Binance API keys"
	@echo "2. Start Redis if running locally: redis-server"
	@echo "3. Run 'make run-dev' to start the service"

# Production deployment
deploy: docker-build
	@echo "Deploying to production..."
	docker-compose -f docker-compose.prod.yml up -d

# Check service health
health:
	@echo "Checking service health..."
	@curl -f http://localhost:8080/health || echo "Service health check endpoint not available"

# Show help
help:
	@echo "Available commands:"
	@echo "  build        - Build the application"
	@echo "  run          - Run the application locally"
	@echo "  run-dev      - Run in development mode"
	@echo "  deps         - Install dependencies"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Start with Docker Compose"
	@echo "  docker-stop  - Stop Docker services"
	@echo "  docker-logs  - View Docker logs"
	@echo "  setup-env    - Setup environment file"
	@echo "  fmt          - Format Go code"
	@echo "  lint         - Lint Go code"
	@echo "  dev-tools    - Install development tools"
	@echo "  dev-setup    - Full development setup"
	@echo "  deploy       - Deploy to production"
	@echo "  health       - Check service health"
	@echo "  help         - Show this help message"