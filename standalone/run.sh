#!/bin/bash

echo "===================================="
echo "    TET Data Service - Quick Start"
echo "===================================="

# 检测操作系统和架构
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case $ARCH in
    x86_64)
        ARCH="amd64"
        ;;
    aarch64|arm64)
        ARCH="arm64"
        ;;
    *)
        echo "Error: Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# 确定可执行文件名
EXECUTABLE="tet-data-service-${OS}-${ARCH}"

echo "Detected OS: $OS"
echo "Detected Architecture: $ARCH"
echo "Looking for executable: $EXECUTABLE"

# 检查是否存在编译好的程序
if [ ! -f "$EXECUTABLE" ]; then
    echo "Error: $EXECUTABLE not found"
    echo "Please run build.sh first to compile the program"
    exit 1
fi

# 设置执行权限
chmod +x "$EXECUTABLE"
echo "Program found: $EXECUTABLE"
echo ""

# 检查配置文件
if [ ! -f "config.txt" ]; then
    echo "Warning: config.txt not found"
    echo "Creating example config file..."
    cp config.example config.txt
    echo ""
    echo "Please edit config.txt with your settings:"
    echo "1. Configure Redis connection settings"
    echo "2. Adjust update parameters as needed"
    echo ""
    echo "You can edit the file with:"
    echo "  nano config.txt    (or vim, gedit, etc.)"
    echo ""
    echo "After editing config.txt, run this script again."
    exit 0
fi

echo "Configuration file found: config.txt"
echo ""

# 加载配置文件
if [ -f "config.txt" ]; then
    # 导出环境变量
    export $(grep -v '^#' config.txt | grep -v '^$' | xargs)
fi

# 检查Redis连接（可选）
echo "Testing Redis connection..."
if command -v redis-cli &> /dev/null; then
    redis-cli ping > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Redis connection: OK"
    else
        echo "Redis connection: Failed or Redis not running"
        echo "Warning: Please ensure Redis is installed and running"
        echo "You can install Redis from: https://redis.io/download"
    fi
else
    echo "Redis CLI not found, skipping connection test"
fi

echo ""
echo "===================================="
echo "      Starting TET Data Service"
echo "===================================="
echo ""
echo "Press Ctrl+C to stop the service"
echo ""

# 构建命令行参数
ARGS=""

if [ ! -z "$REDIS_ADDR" ]; then
    ARGS="$ARGS --redis=$REDIS_ADDR"
fi

if [ ! -z "$REDIS_DB" ]; then
    ARGS="$ARGS --redis-db=$REDIS_DB"
fi

if [ ! -z "$UPDATE_INTERVAL" ]; then
    ARGS="$ARGS --interval=$UPDATE_INTERVAL"
fi

if [ ! -z "$CONCURRENT_REQUESTS" ]; then
    ARGS="$ARGS --concurrent=$CONCURRENT_REQUESTS"
fi

if [ ! -z "$MAX_DATA_AGE" ]; then
    ARGS="$ARGS --max-age=$MAX_DATA_AGE"
fi

if [ ! -z "$TIMEFRAME" ]; then
    ARGS="$ARGS --timeframe=$TIMEFRAME"
fi

if [ ! -z "$SYMBOLS" ]; then
    ARGS="$ARGS --symbols=$SYMBOLS"
fi

# 启动程序
./$EXECUTABLE $ARGS

echo ""
echo "Service stopped."
