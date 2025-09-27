# TET 数据服务

面向 TET 框架的高性能 Go 服务，用于获取与流式处理加密货币市场数据。服务从 Binance U 本位合约拉取 K 线，写入 Redis，并新增了实时订单簿信号、清算事件与持仓量（OI）历史，全栈由 Go 实现，便于一体化部署与维护。

## 特性
- 并发 K 线更新，内置速率限制与自适应调度。
- Redis 缓存与新鲜度监控，系统状态统计。
- 实时订单簿信号（3 个价差区间的深度/压力指标，按分钟聚合）。
- U 本位清算实时流，按分钟聚合多/空清算规模。
- OI 历史拉取与存储，便于波动/换手分析。
- Docker 支持与结构化日志。

## 架构
- `internal/binance/`：U 本位 REST 客户端（klines、exchangeInfo、OI 历史）。
- `internal/updater/`：调度与批量更新（全量/增量策略、对齐刷新）。
- `internal/redis/`：Redis 客户端、K 线存储、ZSET 辅助方法。
- `internal/services/orderbook/`：深度 WS + REST 快照 → 分钟级信号。
- `internal/services/liquidation/`：`!forceOrder@arr` → 多/空分钟桶聚合。
- `internal/services/oi/`：OI 历史轮询 → ZSET 存储。
- `cmd/server/`：入口与运行参数。

## 快速开始
```bash
make dev-setup     # 安装依赖与工具，创建 .env
make run-dev       # 构建并以调试日志运行
# 或
make build && ./bin/tet-data-service --log-level=info
```

## 配置（.env）
- `REDIS_ADDR`（默认 `localhost:6379`）、`REDIS_DB`（默认 `1`）、`REDIS_PASSWORD`。
- `BINANCE_BASE_URL`（默认 `https://fapi.binance.com`）、`API_REQUESTS_PER_SEC`（默认 `15`）。
- `CONCURRENT_REQUESTS`（默认 `12`）、`MAX_DATA_AGE` 秒（默认 `540`）。
- `TIMEFRAME`（默认 `15m`）、`TIMEFRAMES`（逗号分隔）、`SYMBOLS`（`BTC/USDT` 风格）。

## 服务开关（通过 .env 设置）
- 订单簿：`ORDERBOOK_ENABLED=true`，`ORDERBOOK_KEEP_HOURS=2`
- 清算流：`LIQUIDATION_ENABLED=true`，`LIQUIDATION_KEEP_HOURS=24`
- OI 更新：`OI_ENABLED=false`，`OI_PERIOD=5m`，`OI_INTERVAL=300`

示例 `.env` 片段：
```dotenv
ORDERBOOK_ENABLED=true
ORDERBOOK_KEEP_HOURS=2
LIQUIDATION_ENABLED=true
LIQUIDATION_KEEP_HOURS=24
OI_ENABLED=false
OI_PERIOD=5m
OI_INTERVAL=300
VOLATILITY_ENABLED=true
VOLATILITY_TIMEFRAME=1h
VOLATILITY_INTERVAL=300
```

## Docker
```bash
cp .env.example .env
docker-compose up -d
docker-compose logs -f tet-data-service
```

## Redis 键模式
- K 线：`tet:kline:{timeframe}:{symbol}`；时间戳：`tet:timestamp:{timeframe}:{symbol}`；系统状态：`tet:system:status[:timeframe]`。
- 订单簿信号：`orderbook_signals:{symbol}:depth_{i}`（ZSET；score=分钟时间戳；value=JSON）。
- 清算：原始 `liquidation:{symbol}`；聚合 `liquidation_long:{symbol}` 与 `liquidation_short:{symbol}`（按分钟 ZINCRBY）。
- OI：`open_interest:{symbol}`（ZSET；score=毫秒时间戳；value=JSON）。
- 波动性：`tet:volatility:{symbol}`（JSON；TTL 30m）；排行榜：`tet:volatility:ranking`（ZSET；score=综合波动评分）。

## 监控与健康检查
- 日志级别：`--log-level=debug|info|warn|error`。
- 启动时执行 Redis 与 Binance 连接性检查。可用时运行 `make health` 进行 HTTP 健康探测。

## 故障排查
- Redis 连接：检查 `REDIS_ADDR` 与本地服务（`redis-server`）。
- Binance 速率：下调 `API_REQUESTS_PER_SEC` 或减少 `CONCURRENT_REQUESTS`。
- 交易对格式：`.env` 中使用 `BTC/USDT` 风格，内部会自动转换为 Binance 格式。


### Data Structure
```json
{
  "ohlcv_data": "{\"1757002500000\":{\"close\":\"179.34\",\"high\":\"179.36\",\"low\":\"175.55\",\"open\":\"175.89\",\"timestamp\":1757002500000,\"volume\":\"205835.192\"},\"1757006100000\":{\"close\":\"177.6\",\"high\":\"179.66\",\"low\":\"177.21\",\"open\":\"179.35\",\"timestamp\":1757006100000,\"volume\":\"275884.101\"}}",
  "last_candle_time": "2024-01-01T12:00:00.000",
  "last_update": "2024-01-01T12:00:00.000",
  "data_start": "2023-12-02T00:00:00.000",
  "data_end": "2024-01-01T12:00:00.000",
  "data_completeness": 0.95,
  "candle_count": 2880,
  "timeframe": "15m",
  "data_age_seconds": 45.2
}
```

## Performance & Scaling

### Resource Usage
- **Memory**: ~2MB per symbol for 30 days of 15m data
- **CPU**: Minimal, mostly I/O bound
- **Network**: ~10KB per API request
- **Storage**: Redis data with configurable expiration

### Optimization Features
- **Adaptive Intervals**: Automatic adjustment based on market hours and error rates
- **Incremental Updates**: Fetches only new data when possible
- **Batch Processing**: Processes symbols in batches to avoid rate limits
- **Error Recovery**: Exponential backoff with automatic retry
- **Market Hours Optimization**: Reduced frequency during low-activity periods

### Scaling Recommendations
- **Horizontal**: Deploy multiple instances with different symbol sets
- **Vertical**: Increase `CONCURRENT_REQUESTS` based on server capacity
- **Geographic**: Deploy in regions close to Binance servers

## Deployment Options

### Standalone Binary Deployment

**Advantages:**
- Single file deployment
- No dependencies required
- Cross-platform compatibility
- Simple configuration
- Perfect for VPS/cloud servers

**Use cases:**
- Remote server deployment
- Simple production setups
- Testing environments
- Resource-constrained servers

### Makefile Commands (Modular Version)

```bash
make build         # Build the application
make run          # Run locally
make run-dev      # Run in development mode
make test         # Run tests
make docker-build # Build Docker image
make docker-run   # Start with Docker Compose
make docker-stop  # Stop Docker services
make dev-setup    # Complete development setup
make help         # Show all commands
```

### Standalone Build Commands

```bash
# Windows
cd standalone
build.bat

# Linux/macOS
cd standalone
chmod +x build.sh
./build.sh
```

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐
│   Binance API   │◄───┤ TET Data     │───►│   Redis     │
│                 │    │ Service      │    │   Cache     │
└─────────────────┘    └──────────────┘    └─────────────┘
                              │
                              ▼
                       ┌──────────────┐
                       │ TET Analysis │
                       │   Systems    │
                       └──────────────┘
```

### Core Components
- **Config**: Environment-based configuration management
- **Binance Client**: Rate-limited API client with retry logic
- **Redis Client**: Optimized caching with batch operations
- **Data Updater**: Main service orchestrator with adaptive scheduling
- **Logger**: Structured logging with configurable levels

## Error Handling

The service includes comprehensive error handling:

- **API Errors**: Retry with exponential backoff
- **Network Issues**: Automatic reconnection attempts
- **Data Validation**: Verification of fetched data integrity
- **Resource Limits**: Memory and connection monitoring
- **Graceful Shutdown**: Proper cleanup on termination

## Security Considerations

- **Public Data Only**: Service relies on Binance public endpoints; API keys are not required
- **Redis**: Configure authentication if accessible externally
- **Network**: Use VPC/firewall rules to restrict access
- **Monitoring**: Log access attempts and API usage

## License

This project is part of the TET Framework - check the main project for licensing information.

## Examples

### Standalone Deployment Examples

#### Basic Usage
```bash
# Windows - Basic setup
tet-data-service-windows-amd64.exe

# Linux - Remote Redis
./tet-data-service-linux-amd64 \
  --redis=192.168.1.100:6379 \
  --redis-db=2

# Custom symbols and timeframe
./tet-data-service-linux-amd64 \
  --symbols="BTC/USDT,ETH/USDT,BNB/USDT" \
  --timeframe=5m \
  --interval=60
```

#### Server Deployment
```bash
# Run in background (Linux)
nohup ./tet-data-service-linux-amd64 \
  --redis=localhost:6379 \
  --interval=180 > tet-service.log 2>&1 &

# Create systemd service
sudo systemctl enable tet-data-service
sudo systemctl start tet-data-service
```

#### Configuration File Method
```bash
# Create config.txt with your settings
echo "REDIS_ADDR=localhost:6379" > config.txt
echo "TIMEFRAME=15m" >> config.txt

# Run with config file
./tet-data-service-linux-amd64 --config=config.txt
```

### File Structure

```
tet-data-service/
├── standalone/                 # Standalone compilation version
│   ├── main.go                # Single-file implementation
│   ├── go.mod                 # Dependencies
│   ├── build.bat             # Windows build script
│   ├── build.sh              # Linux/macOS build script
│   ├── run.bat               # Windows startup script
│   ├── run.sh                # Linux/macOS startup script
│   ├── config.example        # Configuration template
│   └── README.md             # Standalone version guide
├── cmd/server/               # Modular version entry point
├── internal/                 # Modular version packages
├── docker-compose.yml        # Docker deployment
├── Makefile                  # Build automation
└── README.md                 # This file
```

## Support

For issues and feature requests, please refer to the main TET Framework repository.

### Getting Help

1. **Standalone Version**: Check `standalone/README.md` for detailed instructions
2. **Docker Version**: Use `docker-compose logs -f tet-data-service` for troubleshooting
3. **Configuration Issues**: Verify API keys and Redis connection settings
4. **Performance Tuning**: Adjust `--concurrent` and `--interval` parameters based on your server capacity
