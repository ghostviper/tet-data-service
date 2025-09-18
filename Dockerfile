# Build stage
FROM golang:1.21-alpine AS builder

# Install dependencies
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o tet-data-service ./cmd/server

# Final stage
FROM alpine:latest

# Install certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

# Create app user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /root/

# Copy binary from builder stage
COPY --from=builder /app/tet-data-service .

# Copy configuration example
COPY --from=builder /app/.env.example .

# Change ownership
RUN chown -R appuser:appgroup /root/

# Switch to app user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep tet-data-service || exit 1

# Expose port (if needed for future web interface)
EXPOSE 8080

# Command to run
CMD ["./tet-data-service"]