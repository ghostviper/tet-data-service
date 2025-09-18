# TET Data Service - 独立编译版本

这是 TET 数据服务的独立编译版本，包含单个可执行文件和简化的配置方式。

## 快速开始

### 1. 编译程序

#### Windows 用户
```bash
# 双击运行或在命令行执行
build.bat
```

#### Linux/macOS 用户
```bash
# 赋予执行权限并运行
chmod +x build.sh
./build.sh
```

编译完成后，会生成多个平台的可执行文件：
- `tet-data-service-windows-amd64.exe` (Windows 64位)
- `tet-data-service-linux-amd64` (Linux 64位)
- `tet-data-service-linux-arm64` (Linux ARM64)
- `tet-data-service-darwin-amd64` (macOS Intel)
- `tet-data-service-darwin-arm64` (macOS M1/M2)

### 2. 配置服务

复制配置示例文件：
```bash
# Windows
copy config.example config.txt

# Linux/macOS
cp config.example config.txt
```

编辑 `config.txt` 文件，设置必要的配置：
```ini
# 必须设置的 Binance API 凭据
BINANCE_API_KEY=你的API密钥
BINANCE_SECRET_KEY=你的密钥密码

# Redis 连接（默认本地）
REDIS_ADDR=localhost:6379
REDIS_DB=1

# 可选：自定义更新参数
UPDATE_INTERVAL=180
CONCURRENT_REQUESTS=12
TIMEFRAME=15m
```

### 3. 启动服务

#### 使用启动脚本（推荐）

**Windows:**
```bash
# 双击运行或命令行执行
run.bat
```

**Linux/macOS:**
```bash
chmod +x run.sh
./run.sh
```

#### 直接运行可执行文件

**Windows:**
```bash
tet-data-service-windows-amd64.exe --api-key=你的密钥 --secret-key=你的密码
```

**Linux:**
```bash
./tet-data-service-linux-amd64 --api-key=你的密钥 --secret-key=你的密码
```

**macOS:**
```bash
./tet-data-service-darwin-amd64 --api-key=你的密钥 --secret-key=你的密码
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--api-key` | - | Binance API Key (必需) |
| `--secret-key` | - | Binance Secret Key (必需) |
| `--redis` | localhost:6379 | Redis 服务器地址 |
| `--redis-db` | 1 | Redis 数据库编号 |
| `--interval` | 180 | 更新间隔（秒） |
| `--concurrent` | 12 | 并发请求数 |
| `--max-age` | 540 | 数据最大过期时间（秒） |
| `--timeframe` | 15m | K线时间框架 |
| `--symbols` | - | 自定义交易对（逗号分隔） |

## 环境变量配置

你也可以通过环境变量设置配置：

```bash
# Windows
set BINANCE_API_KEY=你的密钥
set BINANCE_SECRET_KEY=你的密码
set REDIS_ADDR=localhost:6379

# Linux/macOS
export BINANCE_API_KEY=你的密钥
export BINANCE_SECRET_KEY=你的密码
export REDIS_ADDR=localhost:6379
```

## 使用示例

### 基本使用
```bash
# 使用默认设置（需要先设置环境变量或config.txt）
./tet-data-service-linux-amd64

# 使用命令行参数
./tet-data-service-linux-amd64 \
  --api-key=你的密钥 \
  --secret-key=你的密码 \
  --redis=localhost:6379 \
  --interval=120
```

### 高级配置
```bash
# 自定义交易对和更新频率
./tet-data-service-linux-amd64 \
  --api-key=你的密钥 \
  --secret-key=你的密码 \
  --symbols="BTC/USDT,ETH/USDT,BNB/USDT" \
  --interval=60 \
  --concurrent=20 \
  --timeframe=5m
```

### 远程 Redis
```bash
# 连接到远程 Redis 服务器
./tet-data-service-linux-amd64 \
  --api-key=你的密钥 \
  --secret-key=你的密码 \
  --redis=192.168.1.100:6379 \
  --redis-db=2
```

## 部署建议

### 服务器部署

1. **上传可执行文件**：将对应平台的可执行文件上传到服务器
2. **设置配置**：创建 `config.txt` 或设置环境变量
3. **后台运行**：
   ```bash
   # Linux 后台运行
   nohup ./tet-data-service-linux-amd64 > tet.log 2>&1 &

   # 查看进程
   ps aux | grep tet-data-service

   # 停止服务
   pkill tet-data-service
   ```

### 系统服务（Linux）

创建 systemd 服务文件 `/etc/systemd/system/tet-data-service.service`：
```ini
[Unit]
Description=TET Data Service
After=network.target redis.service

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/tet-data-service
ExecStart=/path/to/tet-data-service/tet-data-service-linux-amd64
Environment=BINANCE_API_KEY=你的密钥
Environment=BINANCE_SECRET_KEY=你的密码
Environment=REDIS_ADDR=localhost:6379
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

启用和管理服务：
```bash
sudo systemctl daemon-reload
sudo systemctl enable tet-data-service
sudo systemctl start tet-data-service
sudo systemctl status tet-data-service
```

### Docker 运行

如果你更喜欢使用 Docker：
```dockerfile
FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY tet-data-service-linux-amd64 /usr/local/bin/tet-data-service
COPY config.txt /config.txt
CMD ["tet-data-service", "--config", "/config.txt"]
```

## 监控和日志

### 检查服务状态
程序会输出详细的运行日志，包括：
- 连接状态
- 更新进度
- 错误信息
- 性能统计

### Redis 数据检查
```bash
# 连接到 Redis 检查数据
redis-cli

# 查看所有 TET 相关键
KEYS tet:*

# 查看特定交易对数据
GET tet:kline:15m:BTC_USDT

# 查看系统状态
GET tet:system:status
```

## 故障排除

### 常见问题

1. **API 密钥错误**
   - 确保 API 密钥和密码正确
   - 检查 Binance API 权限设置

2. **Redis 连接失败**
   - 确保 Redis 服务正在运行
   - 检查 Redis 地址和端口
   - 验证 Redis 访问权限

3. **网络连接问题**
   - 检查网络连接到 Binance API
   - 确认防火墙设置
   - 如果在中国大陆，可能需要使用代理

4. **权限问题**
   - 确保可执行文件有执行权限
   - Linux/macOS 用户运行: `chmod +x tet-data-service-*`

### 调试模式

添加详细日志输出：
```bash
./tet-data-service-linux-amd64 --api-key=你的密钥 --secret-key=你的密码 -v
```

## 性能优化

### 推荐配置

**小型部署（1-25个交易对）:**
```
UPDATE_INTERVAL=180
CONCURRENT_REQUESTS=8
API_REQUESTS_PER_SEC=10
```

**中型部署（25-50个交易对）:**
```
UPDATE_INTERVAL=120
CONCURRENT_REQUESTS=12
API_REQUESTS_PER_SEC=15
```

**大型部署（50+个交易对）:**
```
UPDATE_INTERVAL=90
CONCURRENT_REQUESTS=20
API_REQUESTS_PER_SEC=20
```

### 资源使用

- **CPU**: 低使用率，主要是 I/O 等待
- **内存**: 每个交易对约 2MB（30天15分钟数据）
- **网络**: 每次API请求约 10KB
- **存储**: Redis 数据，可配置过期时间

## 支持和更新

这是独立编译版本，包含所有必要功能但不包含 TET 核心分析算法。如需完整的 TET 框架功能，请参考主项目文档。

### 版本信息
- 构建时间：编译时自动生成
- Go 版本：Go 1.21+
- 支持平台：Windows, Linux, macOS (AMD64/ARM64)