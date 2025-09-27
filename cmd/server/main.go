package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "tet-data-service/internal/config"
    "tet-data-service/internal/services/liquidation"
    "tet-data-service/internal/services/oi"
    "tet-data-service/internal/services/orderbook"
    "tet-data-service/internal/services/volatility"
    "tet-data-service/internal/updater"
    "tet-data-service/pkg/logger"

    "github.com/sirupsen/logrus"
)

func main() {
	// 命令行参数
    var (
        configPath = flag.String("config", ".env", "Configuration file path")
        concurrent = flag.Int("concurrent", 12, "Concurrent request count")
        maxAge     = flag.Int("max-age", 540, "Maximum data age in seconds")
        timeframe  = flag.String("timeframe", "15m", "Single timeframe override (deprecated, use --timeframes)")
        timeframes = flag.String("timeframes", "", "Comma-separated list of timeframes (e.g. 1m,5m,15m)")
        logLevel   = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
        // No per-service flags; use .env toggles in config
    )
	flag.Parse()

	// 初始化日志
	if err := logger.Init(*logLevel); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		logrus.Fatalf("Failed to load configuration: %v", err)
	}

	// 应用命令行参数覆盖配置
	if *concurrent != 12 {
		cfg.ConcurrentRequests = *concurrent
	}
	if *maxAge != 540 {
		cfg.MaxDataAge = time.Duration(*maxAge) * time.Second
	}
	if *timeframes != "" {
		cfg.Timeframes = parseCLIList(*timeframes)
		if len(cfg.Timeframes) > 0 {
			cfg.Timeframe = cfg.Timeframes[0]
		}
	} else if *timeframe != "15m" {
		cfg.Timeframes = []string{*timeframe}
		cfg.Timeframe = *timeframe
	}
	if len(cfg.Timeframes) == 0 {
		cfg.Timeframes = []string{cfg.Timeframe}
	}

	logrus.Info("Starting TET Real-Time Data Service...")
	logrus.Infof("Configuration: concurrent=%d, max-age=%v, timeframes=%v",
		cfg.ConcurrentRequests, cfg.MaxDataAge, cfg.Timeframes)

	// 创建更新服务
	dataUpdater, err := updater.New(cfg)
	if err != nil {
		logrus.Fatalf("Failed to create data updater: %v", err)
	}
	defer dataUpdater.Close()

	// 启动前健康检查
	ctx := context.Background()
	if err := dataUpdater.HealthCheck(ctx); err != nil {
		logrus.Warnf("Health check failed: %v", err)
	}

	// 显示资源使用估算
	usage := dataUpdater.GetResourceUsage()
	logrus.Info("=== Resource Usage Estimation ===")
	logrus.Infof("API calls per hour: %.0f", usage.APICallsPerHour)
	logrus.Infof("Estimated memory usage: %dMB", usage.EstimatedMemoryMB)
	logrus.Infof("Estimated bandwidth: %.1fMB/hour", usage.BandwidthPerHourMB)

	// 设置优雅关闭
	ctx, cancel := context.WithCancel(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logrus.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// 启动数据更新服务
	// Start fused Python-equivalent services in Go
    if cfg.OrderbookEnabled {
        ob := orderbook.New(dataUpdater.Redis(), cfg.Symbols, cfg.OrderbookKeepHours)
        _ = ob.Start(ctx)
    }
    if cfg.LiquidationEnabled {
        liq := liquidation.New(dataUpdater.Redis(), cfg.LiquidationKeepHours)
        _ = liq.Start(ctx)
    }
    if cfg.OIEnabled {
        oiSvc := oi.New(dataUpdater.Binance(), dataUpdater.Redis(), cfg.Symbols, cfg.OIPeriod, cfg.OIIntervalSec)
        _ = oiSvc.Start(ctx)
    }
    if cfg.VolatilityEnabled {
        vol := volatility.New(dataUpdater.Redis(), cfg.Symbols, cfg.VolatilityTimeframe, cfg.VolatilityIntervalSec)
        _ = vol.Start(ctx)
    }

	if err := dataUpdater.Start(ctx); err != nil {
		logrus.Errorf("Data updater stopped with error: %v", err)
		os.Exit(1)
	}

	logrus.Info("TET Real-Time Data Service stopped")
}

func parseCLIList(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		clean := strings.TrimSpace(part)
		if clean != "" {
			result = append(result, clean)
		}
	}
	return result
}
