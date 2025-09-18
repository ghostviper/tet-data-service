package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tet-data-service/internal/config"
	"tet-data-service/internal/updater"
	"tet-data-service/pkg/logger"

	"github.com/sirupsen/logrus"
)

func main() {
	// 命令行参数
	var (
		configPath   = flag.String("config", ".env", "Configuration file path")
		interval     = flag.Int("interval", 180, "Update interval in seconds")
		concurrent   = flag.Int("concurrent", 12, "Concurrent request count")
		maxAge       = flag.Int("max-age", 540, "Maximum data age in seconds")
		timeframe    = flag.String("timeframe", "15m", "Kline timeframe (15m, 30m, 1h, 2h, 4h, 1d)")
		logLevel     = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
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
	if *interval != 180 {
		cfg.UpdateInterval = time.Duration(*interval) * time.Second
	}
	if *concurrent != 12 {
		cfg.ConcurrentRequests = *concurrent
	}
	if *maxAge != 540 {
		cfg.MaxDataAge = time.Duration(*maxAge) * time.Second
	}
	if *timeframe != "15m" {
		cfg.Timeframe = *timeframe
	}

	logrus.Info("Starting TET Real-Time Data Service...")
	logrus.Infof("Configuration: interval=%v, concurrent=%d, max-age=%v, timeframe=%s",
		cfg.UpdateInterval, cfg.ConcurrentRequests, cfg.MaxDataAge, cfg.Timeframe)

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
	if err := dataUpdater.Start(ctx); err != nil {
		logrus.Errorf("Data updater stopped with error: %v", err)
		os.Exit(1)
	}

	logrus.Info("TET Real-Time Data Service stopped")
}