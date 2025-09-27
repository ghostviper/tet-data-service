# Repository Guidelines

## Project Structure & Module Organization
- `cmd/server/`: Service entrypoint (`main.go`).
- `internal/`: Core modules (`binance`, `redis`, `config`, `updater`).
- `pkg/logger/`: Logrus setup and logging helpers.
- `python/`: Optional utilities (websockets, monitoring) not required for Go build.
- Root: `Makefile`, `Dockerfile`, `docker-compose.yml`, `.env.example`, `README.md`.

## Build, Test, and Development Commands
- `make dev-setup`: Install Go deps/tools, create `.env` from example.
- `make build`: Compile to `bin/tet-data-service`.
- `make run-dev`: Run with debug logs and local `.env`.
- `make test`: Run Go tests (`./...`).
- `make fmt` / `make lint`: Format via `gofmt`; lint via `golangci-lint`.
- `make docker-run` / `make docker-logs` / `make docker-stop`: Manage Docker Compose.

### Python → Go Fusion Usage
- 订单簿信号：通过 `.env` 控制。`ORDERBOOK_ENABLED=true`，`ORDERBOOK_KEEP_HOURS=2`。数据键：`orderbook_signals:{symbol}:depth_{i}`。
- 清算：`.env` 控制。`LIQUIDATION_ENABLED=true`，`LIQUIDATION_KEEP_HOURS=24`。原始键：`liquidation:{symbol}`；聚合键：`liquidation_long:{symbol}` / `liquidation_short:{symbol}`。
- OI 更新：`.env` 控制。`OI_ENABLED=false`，`OI_PERIOD=5m|15m|1h|4h|1d`，`OI_INTERVAL=<秒>`。数据键：`open_interest:{symbol}`。

## Coding Style & Naming Conventions
- Go 1.21; always run `make fmt` and `make lint` before pushing.
- Package names: short, lowercase; files: `snake_case.go`.
- Exported identifiers: `CamelCase`; unexported: `lowerCamelCase`.
- Add GoDoc comments for exported types/functions.
- Errors: wrap with context; prefer `fmt.Errorf("...: %w", err)`.
- Logging: use `pkg/logger` (Logrus) with structured fields; choose appropriate level.

## Testing Guidelines
- Tests live alongside code as `*_test.go`; functions `TestXxx` and table-driven tests encouraged.
- Run locally with `make test` or `go test ./... -cover`.
- Avoid external effects: stub Binance calls and use a dedicated Redis DB/index or test container.
- Include tests for new logic paths and regressions; keep tests deterministic.

Fusion logic tests:
- Orderbook signal pressure: verify range aggregation and ZSET writes per minute.
- Liquidation flow: ensure long/short minute buckets and retention cleanup.
- OI history: verify pagination window and ZSET storage with 24h cleanup.

## Commit & Pull Request Guidelines
- Use Conventional Commits: `feat(scope): message`, `fix(scope): ...`, `refactor(scope): ...`, `chore: ...`, `docs: ...` (e.g., `feat(updater): align 15m refresh`).
- Scope should map to directories/modules (`updater`, `config`, `redis`, `binance`).
- PRs: concise description, rationale, screenshots/logs if relevant, and linked issues. Confirm `make fmt lint test` passes.

## Security & Configuration Tips
- Never commit secrets. Use `.env` locally; reference `.env.example` for keys like `BINANCE_*` and `REDIS_*`.
- Prefer `make setup-env` and `make docker-run` for a reproducible environment.
- When changing flags or env vars, update `README.md` and `.env.example` accordingly.

## Agent-Specific Notes
- Keep diffs minimal and focused; avoid renames unless necessary.
- Follow the existing structure and error/logging patterns.
- If you touch build or config, update docs and Makefile targets to stay consistent.
