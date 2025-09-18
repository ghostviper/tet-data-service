@echo off
echo ====================================
echo   TET Data Service - Build Script
echo ====================================

:: 检查Go环境
go version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Go is not installed or not in PATH
    echo Please install Go from https://golang.org/dl/
    pause
    exit /b 1
)

echo Go environment detected
go version

:: 设置变量
set APP_NAME=tet-data-service
set BUILD_TIME=%date% %time%
set GO_VERSION=

for /f "tokens=3" %%i in ('go version') do set GO_VERSION=%%i

echo.
echo Building %APP_NAME%...
echo Build time: %BUILD_TIME%
echo Go version: %GO_VERSION%

:: 下载依赖
echo.
echo Downloading dependencies...
go mod download
if %errorlevel% neq 0 (
    echo Error: Failed to download dependencies
    pause
    exit /b 1
)

go mod tidy
if %errorlevel% neq 0 (
    echo Error: Failed to tidy modules
    pause
    exit /b 1
)

:: 编译 Windows 版本
echo.
echo Compiling for Windows (amd64)...
set GOOS=windows
set GOARCH=amd64
set CGO_ENABLED=0
go build -ldflags "-s -w -X main.buildTime=%BUILD_TIME%" -o %APP_NAME%-windows-amd64.exe main.go
if %errorlevel% neq 0 (
    echo Error: Failed to build Windows version
    pause
    exit /b 1
)

echo Windows version built: %APP_NAME%-windows-amd64.exe

:: 编译 Linux 版本
echo.
echo Compiling for Linux (amd64)...
set GOOS=linux
set GOARCH=amd64
set CGO_ENABLED=0
go build -ldflags "-s -w -X main.buildTime=%BUILD_TIME%" -o %APP_NAME%-linux-amd64 main.go
if %errorlevel% neq 0 (
    echo Error: Failed to build Linux version
    pause
    exit /b 1
)

echo Linux version built: %APP_NAME%-linux-amd64

:: 编译 macOS 版本
echo.
echo Compiling for macOS (amd64)...
set GOOS=darwin
set GOARCH=amd64
set CGO_ENABLED=0
go build -ldflags "-s -w -X main.buildTime=%BUILD_TIME%" -o %APP_NAME%-darwin-amd64 main.go
if %errorlevel% neq 0 (
    echo Error: Failed to build macOS version
    pause
    exit /b 1
)

echo macOS version built: %APP_NAME%-darwin-amd64

:: 编译 macOS ARM64 版本 (M1/M2)
echo.
echo Compiling for macOS (arm64)...
set GOOS=darwin
set GOARCH=arm64
set CGO_ENABLED=0
go build -ldflags "-s -w -X main.buildTime=%BUILD_TIME%" -o %APP_NAME%-darwin-arm64 main.go
if %errorlevel% neq 0 (
    echo Error: Failed to build macOS ARM64 version
    pause
    exit /b 1
)

echo macOS ARM64 version built: %APP_NAME%-darwin-arm64

:: 显示文件大小
echo.
echo ====================================
echo Build completed successfully!
echo ====================================
echo.
echo Built files:
dir /B tet-data-service-*
echo.

:: 显示文件大小详情
for %%f in (tet-data-service-*) do (
    echo %%f - %%~zf bytes
)

echo.
echo Usage examples:
echo   Windows: %APP_NAME%-windows-amd64.exe --help
echo   Linux:   ./%APP_NAME%-linux-amd64 --help
echo   macOS:   ./%APP_NAME%-darwin-amd64 --help
echo.
echo Next steps:
echo 1. Copy the appropriate binary to your target server
echo 2. Set environment variables or use command line flags
echo 3. Run the service with your configuration
echo.

pause