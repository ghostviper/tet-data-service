package updater

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"tet-data-service/internal/binance"
	"tet-data-service/internal/config"
	redisClient "tet-data-service/internal/redis"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type DataUpdater struct {
	config        *config.Config
	binanceClient *binance.Client
	redisClient   *redisClient.Client
	rateLimiter   *rate.Limiter
	semaphore     chan struct{}
	isRunning     bool
	mu            sync.RWMutex

	// 统计信息
	stats struct {
		sync.RWMutex
		successCount int
		failureCount int
		lastUpdate   time.Time
	}
}

type ResourceUsage struct {
	APICallsPerHour     float64
	EstimatedMemoryMB   int
	BandwidthPerHourMB  float64
}

func New(cfg *config.Config) (*DataUpdater, error) {
	// 创建Binance客户端
	binanceClient := binance.NewClient(
		cfg.BinanceBaseURL,
		cfg.APIRequestsPerSec,
	)

	// 创建Redis客户端
	redisClient := redisClient.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)

	// 创建信号量控制并发
	semaphore := make(chan struct{}, cfg.ConcurrentRequests)
	for i := 0; i < cfg.ConcurrentRequests; i++ {
		semaphore <- struct{}{}
	}

	updater := &DataUpdater{
		config:        cfg,
		binanceClient: binanceClient,
		redisClient:   redisClient,
		rateLimiter:   rate.NewLimiter(rate.Limit(cfg.APIRequestsPerSec), cfg.APIRequestsPerSec),
		semaphore:     semaphore,
	}

	return updater, nil
}

func (u *DataUpdater) Start(ctx context.Context) error {
	u.mu.Lock()
	u.isRunning = true
	u.mu.Unlock()

	logrus.Info("TET Real-Time Data Service started")
	logrus.Infof("Update interval: %v", u.config.UpdateInterval)
	logrus.Infof("Concurrent requests: %d", u.config.ConcurrentRequests)
	logrus.Infof("API rate limit: %d req/sec", u.config.APIRequestsPerSec)
	logrus.Infof("Max data age: %v", u.config.MaxDataAge)
	logrus.Infof("Monitoring %d symbols", len(u.config.Symbols))

	// 首次完整更新
	logrus.Info("Performing initial full data update...")
	successCount, failureCount, err := u.UpdateAllSymbols(ctx)
	if err != nil {
		logrus.Errorf("Initial update failed: %v", err)
	} else {
		logrus.Infof("Initial update completed: %d success, %d failed", successCount, failureCount)
	}

	// 主循环
	ticker := time.NewTicker(u.config.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Context cancelled, stopping data updater")
			u.mu.Lock()
			u.isRunning = false
			u.mu.Unlock()
			return ctx.Err()

		case <-ticker.C:
			loopStart := time.Now()

			// 检查并优先更新过期数据
			staleSymbols, err := u.checkStaleData(ctx)
			if err != nil {
				logrus.Errorf("Failed to check stale data: %v", err)
			} else if len(staleSymbols) > 0 {
				logrus.Infof("Found %d stale symbols, updating urgently", len(staleSymbols))
				u.urgentUpdate(ctx, staleSymbols)
			}

			// 常规更新周期
			if u.isRunning {
				successCount, failureCount, err := u.UpdateAllSymbols(ctx)
				if err != nil {
					logrus.Errorf("Update cycle failed: %v", err)
				} else {
					logrus.Infof("Update cycle completed: %d success, %d failed, duration: %.1fs",
						successCount, failureCount, time.Since(loopStart).Seconds())
				}
			}

			// 动态调整更新间隔
			if u.config.AdaptiveInterval {
				newInterval := u.calculateOptimalInterval()
				if newInterval != u.config.UpdateInterval {
					logrus.Infof("Adjusting update interval: %v -> %v", u.config.UpdateInterval, newInterval)
					u.config.UpdateInterval = newInterval
					ticker.Reset(newInterval)
				}
			}
		}
	}
}

func (u *DataUpdater) UpdateAllSymbols(ctx context.Context) (int, int, error) {
	start := time.Now()
	logrus.Infof("Starting batch update for %d symbols", len(u.config.Symbols))

	var wg sync.WaitGroup
	results := make(chan bool, len(u.config.Symbols))
	failedSymbols := make([]string, 0)
	var failedMu sync.Mutex

	// 分批处理避免过多并发请求
	batchSize := u.config.BatchSize
	for i := 0; i < len(u.config.Symbols); i += batchSize {
		end := i + batchSize
		if end > len(u.config.Symbols) {
			end = len(u.config.Symbols)
		}

		batch := u.config.Symbols[i:end]
		logrus.Infof("Processing batch %d: %v", i/batchSize+1, batch)

		// 批次内并发处理
		for _, symbol := range batch {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				success := u.rateLimitedUpdate(ctx, sym)
				results <- success
				if !success {
					failedMu.Lock()
					failedSymbols = append(failedSymbols, sym)
					failedMu.Unlock()
				}
			}(symbol)
		}

		// 等待当前批次完成
		wg.Wait()

		// 批次间延迟
		if end < len(u.config.Symbols) {
			time.Sleep(2 * time.Second)
		}
	}

	close(results)

	// 统计结果
	successCount := 0
	failureCount := 0
	for result := range results {
		if result {
			successCount++
		} else {
			failureCount++
		}
	}

	elapsed := time.Since(start)

	// 更新统计信息
	u.stats.Lock()
	u.stats.successCount = successCount
	u.stats.failureCount = failureCount
	u.stats.lastUpdate = time.Now()
	u.stats.Unlock()

	// 更新系统状态
	avgFreshness := u.redisClient.CalculateAverageFreshness(ctx, u.config.Symbols, u.config.Timeframe)
	status := &redisClient.SystemStatus{
		LastFullScan:        time.Now().Format(time.RFC3339),
		ActiveSymbols:       len(u.config.Symbols),
		SuccessfulUpdates:   successCount,
		FailedUpdates:       failureCount,
		UpdateDuration:      elapsed.Seconds(),
		DataFreshnessAvg:    avgFreshness,
	}

	if err := u.redisClient.UpdateSystemStatus(ctx, status); err != nil {
		logrus.Warnf("Failed to update system status: %v", err)
	}

	if len(failedSymbols) > 0 {
		logrus.Warnf("Failed symbols: %v", failedSymbols[:min(len(failedSymbols), 10)])
		if len(failedSymbols) > 10 {
			logrus.Warnf("... and %d more", len(failedSymbols)-10)
		}
	}

	return successCount, failureCount, nil
}

func (u *DataUpdater) rateLimitedUpdate(ctx context.Context, symbol string) bool {
	// 获取信号量
	select {
	case <-u.semaphore:
		defer func() { u.semaphore <- struct{}{} }()
	case <-ctx.Done():
		return false
	}

	// 应用速率限制
	if err := u.rateLimiter.Wait(ctx); err != nil {
		logrus.Errorf("%s: Rate limit error: %v", symbol, err)
		return false
	}

	return u.fetchAndStoreData(ctx, symbol, false, 3)
}

func (u *DataUpdater) fetchAndStoreData(ctx context.Context, symbol string, forceFull bool, maxRetries int) bool {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := u.doFetchAndStore(ctx, symbol, forceFull); err != nil {
			logrus.Warnf("%s: Attempt %d/%d failed: %v", symbol, attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return false
		}
		return true
	}
	return false
}

func (u *DataUpdater) doFetchAndStore(ctx context.Context, symbol string, forceFull bool) error {
	if !forceFull {
		// 尝试增量更新
		if u.canDoIncrementalUpdate(ctx, symbol) {
			if err := u.doIncrementalUpdate(ctx, symbol); err == nil {
				logrus.Debugf("%s: Incremental update completed", symbol)
				return nil
			} else {
				logrus.Warnf("%s: Incremental update failed, falling back to full: %v", symbol, err)
			}
		}
	}

	// 执行完整更新
	return u.doFullUpdate(ctx, symbol)
}

func (u *DataUpdater) canDoIncrementalUpdate(ctx context.Context, symbol string) bool {
	lastUpdate, err := u.redisClient.GetLastUpdateTime(ctx, symbol, u.config.Timeframe)
	if err != nil || lastUpdate == nil {
		return false
	}

	// 检查数据是否足够新
	age := time.Since(*lastUpdate)
	return age < u.config.MaxDataAge
}

func (u *DataUpdater) doIncrementalUpdate(ctx context.Context, symbol string) error {
	// 获取最新的几根K线
	klines, err := u.binanceClient.GetKlines(ctx, symbol, u.config.Timeframe, u.config.IncrementalCandles, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch incremental klines: %w", err)
	}

	if len(klines) == 0 {
		return fmt.Errorf("no incremental data received")
	}

	// 转换并存储数据
	return u.storeKlineData(ctx, symbol, klines)
}

func (u *DataUpdater) doFullUpdate(ctx context.Context, symbol string) error {
	// 尝试不同的天数
	dayOptions := []int{u.config.DataDays, 15, 7}

	for _, days := range dayOptions {
		logrus.Debugf("%s: Attempting to fetch %d days of historical data", symbol, days)

		klines, err := u.binanceClient.GetHistoricalData(ctx, symbol, u.config.Timeframe, days)
		if err != nil {
			logrus.Warnf("%s: Failed to fetch %d days of data: %v", symbol, days, err)
			continue
		}

		if len(klines) >= 10 {
			if err := u.storeKlineData(ctx, symbol, klines); err != nil {
				return fmt.Errorf("failed to store data: %w", err)
			}
			logrus.Infof("%s: Full update completed with %d candles (%d days)", symbol, len(klines), days)
			return nil
		}

		logrus.Warnf("%s: Insufficient data for %d days: %d candles", symbol, days, len(klines))
	}

	return fmt.Errorf("failed to fetch sufficient data with all day options")
}

func (u *DataUpdater) storeKlineData(ctx context.Context, symbol string, klines []binance.KlineItem) error {
	if len(klines) == 0 {
		return fmt.Errorf("no kline data to store")
	}

	// 转换为JSON格式存储（简化版，只存储基本OHLCV数据）
	ohlcvMap := make(map[string]interface{})
	for _, kline := range klines {
		timestamp := time.UnixMilli(kline.OpenTime).Format(time.RFC3339)
		ohlcvMap[timestamp] = map[string]interface{}{
			"open":   kline.Open.String(),
			"high":   kline.High.String(),
			"low":    kline.Low.String(),
			"close":  kline.Close.String(),
			"volume": kline.Volume.String(),
		}
	}

	ohlcvData, err := json.Marshal(ohlcvMap)
	if err != nil {
		return fmt.Errorf("failed to marshal OHLCV data: %w", err)
	}

	// 构建存储数据
	now := time.Now()
	lastKline := klines[len(klines)-1]
	dataEndTime := time.UnixMilli(lastKline.CloseTime)
	dataAge := now.Sub(dataEndTime).Seconds()

	klineData := &redisClient.KlineData{
		OHLCVData:      string(ohlcvData),
		LastCandleTime: dataEndTime.Format(time.RFC3339),
		LastUpdate:     now.Format(time.RFC3339),
		DataStart:      time.UnixMilli(klines[0].OpenTime).Format(time.RFC3339),
		DataEnd:        dataEndTime.Format(time.RFC3339),
		Completeness:   float64(len(klines)) / (float64(u.config.DataDays) * 24 * 4), // 假设15分钟K线
		CandleCount:    len(klines),
		Timeframe:      u.config.Timeframe,
		DataAgeSeconds: dataAge,
	}

	return u.redisClient.StoreKlineData(ctx, symbol, u.config.Timeframe, klineData)
}

func (u *DataUpdater) checkStaleData(ctx context.Context) ([]string, error) {
	return u.redisClient.CheckDataFreshness(ctx, u.config.Symbols, u.config.Timeframe, u.config.MaxDataAge)
}

func (u *DataUpdater) urgentUpdate(ctx context.Context, symbols []string) {
	if len(symbols) == 0 {
		return
	}

	logrus.Infof("Urgent update for stale data: %v", symbols)

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			u.rateLimitedUpdate(ctx, sym)
		}(symbol)
	}

	wg.Wait()
}

func (u *DataUpdater) calculateOptimalInterval() time.Duration {
	currentHour := time.Now().Hour()

	// 交易活跃时段
	peakHours := []int{7, 8, 9, 13, 14, 15, 21, 22}
	isPeakHour := false
	for _, hour := range peakHours {
		if currentHour == hour {
			isPeakHour = true
			break
		}
	}

	baseInterval := u.config.UpdateInterval

	// 非活跃时段延长间隔
	if u.config.MarketHoursOptimization && !isPeakHour {
		baseInterval = time.Duration(float64(baseInterval) * u.config.OffPeakMultiplier)
	}

	// 根据错误率调整
	u.stats.RLock()
	errorRate := float64(u.stats.failureCount) / float64(u.stats.successCount + u.stats.failureCount)
	u.stats.RUnlock()

	if errorRate > 0.2 {
		baseInterval = time.Duration(float64(baseInterval) * u.config.ErrorBackoffMultiplier)
	} else if errorRate < 0.05 {
		baseInterval = time.Duration(float64(baseInterval) * u.config.SuccessRecoveryRate)
	}

	// 最小1分钟间隔
	minInterval := time.Minute
	if baseInterval < minInterval {
		baseInterval = minInterval
	}

	return baseInterval
}

func (u *DataUpdater) GetResourceUsage() ResourceUsage {
	symbolCount := len(u.config.Symbols)
	callsPerHour := (3600.0 / u.config.UpdateInterval.Seconds()) * float64(symbolCount)

	return ResourceUsage{
		APICallsPerHour:     callsPerHour,
		EstimatedMemoryMB:   symbolCount * 2, // 每个符号约2MB
		BandwidthPerHourMB:  (callsPerHour * 10) / 1024, // 每次请求约10KB
	}
}

func (u *DataUpdater) HealthCheck(ctx context.Context) error {
	// 测试Redis连接
	if err := u.redisClient.Ping(ctx); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}

	// 测试Binance连接
	if err := u.binanceClient.TestConnectivity(ctx); err != nil {
		return fmt.Errorf("binance health check failed: %w", err)
	}

	// 验证至少一个交易对
	if len(u.config.Symbols) > 0 {
		valid, err := u.binanceClient.ValidateSymbol(ctx, u.config.Symbols[0])
		if err != nil {
			return fmt.Errorf("symbol validation failed: %w", err)
		}
		if !valid {
			return fmt.Errorf("test symbol %s is not valid", u.config.Symbols[0])
		}
	}

	logrus.Info("Health check passed")
	return nil
}

func (u *DataUpdater) Close() error {
	u.mu.Lock()
	u.isRunning = false
	u.mu.Unlock()

	return u.redisClient.Close()
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (u *DataUpdater) GetStats() map[string]interface{} {
	u.stats.RLock()
	defer u.stats.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]interface{}{
		"success_count": u.stats.successCount,
		"failure_count": u.stats.failureCount,
		"last_update":   u.stats.lastUpdate.Format(time.RFC3339),
		"memory_mb":     memStats.Alloc / 1024 / 1024,
		"goroutines":    runtime.NumGoroutine(),
		"is_running":    u.isRunning,
	}
}
