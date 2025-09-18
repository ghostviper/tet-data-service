#!/bin/bash

echo "===================================="
echo "   TET Data Service - Build Script"
echo "===================================="

# 检查Go环境
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed or not in PATH"
    echo "Please install Go from https://golang.org/dl/"
    exit 1
fi

echo "Go environment detected"
go version

# 设置变量
APP_NAME="tet-data-service"
BUILD_TIME=$(date)
GO_VERSION=$(go version | awk '{print $3}')

echo ""
echo "Building $APP_NAME..."
echo "Build time: $BUILD_TIME"
echo "Go version: $GO_VERSION"

# 下载依赖
echo ""
echo "Downloading dependencies..."
go mod download
if [ $? -ne 0 ]; then
    echo "Error: Failed to download dependencies"
    exit 1
fi

go mod tidy
if [ $? -ne 0 ]; then
    echo "Error: Failed to tidy modules"
    exit 1
fi

# 编译不同平台版本
platforms=(
    "windows/amd64"
    "linux/amd64"
    "linux/arm64"
    "darwin/amd64"
    "darwin/arm64"
)

for platform in "${platforms[@]}"; do
    platform_split=(${platform//\// })
    GOOS=${platform_split[0]}
    GOARCH=${platform_split[1]}

    echo ""
    echo "Compiling for $GOOS ($GOARCH)..."

    output_name="$APP_NAME-$GOOS-$GOARCH"
    if [ $GOOS = "windows" ]; then
        output_name="$output_name.exe"
    fi

    env GOOS=$GOOS GOARCH=$GOARCH CGO_ENABLED=0 go build \
        -ldflags "-s -w -X main.buildTime='$BUILD_TIME'" \
        -o $output_name main.go

    if [ $? -ne 0 ]; then
        echo "Error: Failed to build $GOOS/$GOARCH version"
        exit 1
    fi

    echo "$GOOS/$GOARCH version built: $output_name"
done

# 显示构建结果
echo ""
echo "===================================="
echo "Build completed successfully!"
echo "===================================="
echo ""
echo "Built files:"
ls -la tet-data-service-*

echo ""
echo "File sizes:"
for file in tet-data-service-*; do
    if [ -f "$file" ]; then
        size=$(du -h "$file" | cut -f1)
        echo "$file - $size"
    fi
done

echo ""
echo "Usage examples:"
echo "  Windows: ./tet-data-service-windows-amd64.exe --help"
echo "  Linux:   ./tet-data-service-linux-amd64 --help"
echo "  macOS:   ./tet-data-service-darwin-amd64 --help"
echo ""
echo "Next steps:"
echo "1. Copy the appropriate binary to your target server"
echo "2. Set environment variables or use command line flags"
echo "3. Run the service with your configuration"
echo ""

# 设置执行权限
chmod +x tet-data-service-*

echo "All binaries are ready to use!"