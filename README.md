# TET Data Service

A high-performance, standalone data fetching service for the TET Framework. This Go-based service is designed to be deployed independently to fetch cryptocurrency market data from Binance and store it in Redis for use by TET analysis systems.

## Features

- **High-Performance Data Fetching**: Concurrent data fetching with configurable rate limiting
- **Intelligent Caching**: Redis-based caching with data freshness monitoring
- **Adaptive Intervals**: Dynamic update interval adjustment based on market conditions
- **Comprehensive Monitoring**: Built-in health checks and performance metrics
- **Production Ready**: Docker containerization with proper error handling and logging
- **Scalable Architecture**: Designed for easy deployment across multiple servers

## Quick Start

### Method 1: Standalone Compilation (Recommended for Simple Deployment)

This method creates a single executable file that can run anywhere without dependencies.

1. **Compile the application:**
```bash
# Navigate to standalone directory
cd tet-data-service/standalone

# Windows users
build.bat

# Linux/macOS users
chmod +x build.sh
./build.sh
```

This creates platform-specific executables:
- `tet-data-service-windows-amd64.exe` (Windows 64-bit)
- `tet-data-service-linux-amd64` (Linux 64-bit)
- `tet-data-service-darwin-amd64` (macOS Intel)
- `tet-data-service-darwin-arm64` (macOS M1/M2)

2. **Configure the service:**
```bash
# Copy configuration template
cp config.example config.txt

# Edit with your settings (required: Binance API keys)
notepad config.txt  # Windows
nano config.txt     # Linux/macOS
```

3. **Run the service:**
```bash
# Using startup scripts (recommended)
# Windows:
run.bat

# Linux/macOS:
chmod +x run.sh
./run.sh

# Or run directly:
# Windows:
tet-data-service-windows-amd64.exe --api-key=your_key --secret-key=your_secret

# Linux:
./tet-data-service-linux-amd64 --api-key=your_key --secret-key=your_secret

# macOS:
./tet-data-service-darwin-amd64 --api-key=your_key --secret-key=your_secret
```

### Method 2: Using Docker (Recommended for Production)

1. Clone and setup:
```bash
git clone <repository>
cd tet-data-service
cp .env.example .env
```

2. Edit `.env` file with your Binance API credentials:
```bash
BINANCE_API_KEY=your_api_key_here
BINANCE_SECRET_KEY=your_secret_key_here
```

3. Start the service:
```bash
docker-compose up -d
```

### Method 3: Local Development

1. Install dependencies:
```bash
make dev-setup
```

2. Configure environment:
```bash
# Edit .env file with your configuration
```

3. Run the service:
```bash
make run-dev
```

## Configuration

All configuration is handled through environment variables. See `.env.example` for all available options:

### Core Settings
- `UPDATE_INTERVAL`: Data update frequency (default: 180 seconds)
- `CONCURRENT_REQUESTS`: Number of concurrent API requests (default: 12)
- `MAX_DATA_AGE`: Maximum acceptable data age (default: 540 seconds)
- `TIMEFRAME`: Kline timeframe - 15m, 30m, 1h, 2h, 4h, 1d (default: 15m)

### API Configuration
- `BINANCE_API_KEY`: Your Binance API key (required)
- `BINANCE_SECRET_KEY`: Your Binance secret key (required)
- `API_REQUESTS_PER_SEC`: API rate limit (default: 15/second)

### Redis Configuration
- `REDIS_ADDR`: Redis server address (default: localhost:6379)
- `REDIS_PASSWORD`: Redis password (optional)
- `REDIS_DB`: Redis database number (default: 1)

## Usage

### Standalone Executable Command Line Options

```bash
./tet-data-service-platform-arch [options]

Options:
  --api-key string       Binance API Key (required)
  --secret-key string    Binance Secret Key (required)
  --redis string         Redis server address (default "localhost:6379")
  --redis-db int         Redis database number (default 1)
  --interval int         Update interval in seconds (default 180)
  --concurrent int       Concurrent request count (default 12)
  --max-age int         Maximum data age in seconds (default 540)
  --timeframe string    Kline timeframe (default "15m")
  --symbols string      Custom symbols (comma-separated)
  --config string       Config file path (optional)
```

### Modular Service Command Line Options

```bash
./tet-data-service [options]

Options:
  --config string        Configuration file path (default ".env")
  --interval int         Update interval in seconds (default 180)
  --concurrent int       Concurrent request count (default 12)
  --max-age int         Maximum data age in seconds (default 540)
  --timeframe string    Kline timeframe (default "15m")
  --log-level string    Log level: debug, info, warn, error (default "info")
```

### Docker Deployment

#### Development
```bash
docker-compose up -d
```

#### Production
```bash
docker-compose -f docker-compose.prod.yml up -d
```

### Monitoring

#### View Logs
```bash
# Docker
docker-compose logs -f tet-data-service

# Local
tail -f tet-data-service.log
```

#### Redis Commander (Web UI)
Access Redis web interface at `http://localhost:8081`

#### Health Check
The service includes built-in health checks for:
- Redis connectivity
- Binance API connectivity
- Symbol validation

## Data Storage Format

Data is stored in Redis with the following key patterns:

```
tet:kline:{timeframe}:{symbol}     # OHLCV data
tet:timestamp:{timeframe}:{symbol} # Last update timestamp
tet:system:status                  # System status and metrics
```

### Data Structure
```json
{
  "ohlcv_data": "JSON string of OHLCV data indexed by timestamp",
  "last_candle_time": "2024-01-01T12:00:00Z",
  "last_update": "2024-01-01T12:00:00Z",
  "data_start": "2023-12-02T00:00:00Z",
  "data_end": "2024-01-01T12:00:00Z",
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

- **API Keys**: Store securely using environment variables or secrets management
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
tet-data-service-windows-amd64.exe --api-key=your_key --secret-key=your_secret

# Linux - Remote Redis
./tet-data-service-linux-amd64 \
  --api-key=your_key \
  --secret-key=your_secret \
  --redis=192.168.1.100:6379 \
  --redis-db=2

# Custom symbols and timeframe
./tet-data-service-linux-amd64 \
  --api-key=your_key \
  --secret-key=your_secret \
  --symbols="BTC/USDT,ETH/USDT,BNB/USDT" \
  --timeframe=5m \
  --interval=60
```

#### Server Deployment
```bash
# Run in background (Linux)
nohup ./tet-data-service-linux-amd64 \
  --api-key=your_key \
  --secret-key=your_secret > tet-service.log 2>&1 &

# Create systemd service
sudo systemctl enable tet-data-service
sudo systemctl start tet-data-service
```

#### Configuration File Method
```bash
# Create config.txt with your settings
echo "BINANCE_API_KEY=your_key" > config.txt
echo "BINANCE_SECRET_KEY=your_secret" >> config.txt
echo "REDIS_ADDR=localhost:6379" >> config.txt

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