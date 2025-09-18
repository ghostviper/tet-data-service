@echo off
echo ====================================
echo    TET Data Service - Quick Start
echo ====================================

:: 检查是否存在编译好的程序
if not exist "tet-data-service-windows-amd64.exe" (
    echo Error: tet-data-service-windows-amd64.exe not found
    echo Please run build.bat first to compile the program
    pause
    exit /b 1
)

echo Program found: tet-data-service-windows-amd64.exe
echo.

:: 检查配置文件
if not exist "config.txt" (
    echo Warning: config.txt not found
    echo Creating example config file...
    copy config.example config.txt
    echo.
    echo Please edit config.txt with your settings:
    echo 1. Set your Binance API Key and Secret Key
    echo 2. Configure Redis connection settings
    echo 3. Adjust update parameters as needed
    echo.
    echo Opening config.txt for editing...
    notepad config.txt
    echo.
    echo After editing config.txt, run this script again.
    pause
    exit /b 0
)

echo Configuration file found: config.txt
echo.

:: 检查Redis连接（可选）
echo Testing Redis connection...
redis-cli ping >nul 2>&1
if %errorlevel% equ 0 (
    echo Redis connection: OK
) else (
    echo Redis connection: Failed or Redis not running
    echo Warning: Please ensure Redis is installed and running
    echo You can install Redis from: https://redis.io/download
)

echo.
echo ====================================
echo      Starting TET Data Service
echo ====================================
echo.
echo Press Ctrl+C to stop the service
echo.

:: 设置环境变量并启动程序
for /f "usebackq tokens=1,2 delims==" %%a in ("config.txt") do (
    if not "%%a"=="" if not "%%b"=="" (
        set %%a=%%b
    )
)

:: 启动程序
tet-data-service-windows-amd64.exe ^
    --api-key=%BINANCE_API_KEY% ^
    --secret-key=%BINANCE_SECRET_KEY% ^
    --redis=%REDIS_ADDR% ^
    --redis-db=%REDIS_DB% ^
    --interval=%UPDATE_INTERVAL% ^
    --concurrent=%CONCURRENT_REQUESTS% ^
    --max-age=%MAX_DATA_AGE% ^
    --timeframe=%TIMEFRAME% ^
    --symbols=%SYMBOLS%

echo.
echo Service stopped.
pause