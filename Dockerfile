# syntax=docker/dockerfile:1.7

########################
# Build stage
########################
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

WORKDIR /src

# Cache Go modules separately for faster incremental builds; go.sum may be absent locally
COPY go.mod go.sum* ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Configure target platform (defaults provided by Docker BuildKit)
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH:-amd64}

# GOARM is only relevant for arm architecture
ENV GOARM=${TARGETVARIANT#v}

RUN go build -trimpath -ldflags="-s -w" -o /out/tet-data-service ./cmd/server

########################
# Runtime stage
########################
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -H -s /sbin/nologin tet

WORKDIR /app

COPY --from=builder /out/tet-data-service /app/tet-data-service
COPY .env.example /app/.env.example

USER tet

EXPOSE 8080

ENTRYPOINT ["/app/tet-data-service"]
# Override with `docker run ... --config /app/.env.example` if desired
