package updater

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tet-data-service/internal/binance"
	"tet-data-service/internal/config"
	redisClient "tet-data-service/internal/redis"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// 统一时间格式（CST=UTC+8）
const TimeFormat = "2006-01-02T15:04:05.000"

var cst = time.FixedZone("CST", 8*3600)

type DataUpdater struct {
	config        *config.Config
	binanceClient *binance.Client
	redisClient   *redisClient.Client
	rateLimiter   *rate.Limiter
	semaphore     chan struct{}
	isRunning     bool
	mu            sync.RWMutex
	timeframes    []string

    stats struct {
        sync.RWMutex
        successCount int
        failureCount int
        lastUpdate   time.Time
    }
}

const (
	initialKlineCount = 1000 // 默认大容量，仅用于非分档周期
	maxStoredCandles  = initialKlineCount
)

type ResourceUsage struct {
	APICallsPerHour    float64
	EstimatedMemoryMB  int
	BandwidthPerHourMB float64
}

func New(cfg *config.Config) (*DataUpdater, error) {
	// Binance client
	bc := binance.NewClient(
		cfg.BinanceBaseURL,
		cfg.APIRequestsPerSec,
	)

	// Redis client
	rc := redisClient.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)

	// 并发信号量
	if cfg.ConcurrentRequests <= 0 {
		cfg.ConcurrentRequests = 10
	}
	semaphore := make(chan struct{}, cfg.ConcurrentRequests)
	for i := 0; i < cfg.ConcurrentRequests; i++ {
		semaphore <- struct{}{}
	}

	// 限速器兜底
	if cfg.APIRequestsPerSec <= 0 {
		logrus.Warnf("APIRequestsPerSec=%d is invalid; default to 10 rps", cfg.APIRequestsPerSec)
		cfg.APIRequestsPerSec = 10
	}

	updater := &DataUpdater{
		config:        cfg,
		binanceClient: bc,
		redisClient:   rc,
		rateLimiter:   rate.NewLimiter(rate.Limit(cfg.APIRequestsPerSec), cfg.APIRequestsPerSec),
		semaphore:     semaphore,
	}

	// 解析/校验 TIMEFRAMES
	if len(cfg.Timeframes) == 0 {
		return nil, fmt.Errorf("no timeframes configured")
	}
	unique := make(map[string]struct{})
	for _, raw := range cfg.Timeframes {
		tf := strings.TrimSpace(raw)
		if tf == "" {
			continue
		}
		if _, ok := unique[tf]; ok {
			continue
		}
		if binance.ParseInterval(tf) <= 0 {
			logrus.Warnf("⛔ TIMEFRAMES contains unsupported interval: %q (skipped)", tf)
			continue
		}
		unique[tf] = struct{}{}
		updater.timeframes = append(updater.timeframes, tf)
	}
	if len(updater.timeframes) == 0 {
		return nil, fmt.Errorf("no valid timeframes configured (after validation)")
	}
	logrus.Infof("🧩 Timeframes enabled: %v", updater.timeframes)

	return updater, nil
}

func (u *DataUpdater) Start(ctx context.Context) error {
	u.mu.Lock()
	u.isRunning = true
	u.mu.Unlock()

	logrus.Info("🚀 TET Real-Time Data Service started")
	logrus.Infof("⚙️  Config | Concurrent: %d", u.config.ConcurrentRequests)
	logrus.Infof("⚙️  Config | Rate limit: %d rps", u.config.APIRequestsPerSec)
	logrus.Infof("⚙️  Config | Max data age: %v", u.config.MaxDataAge)
	logrus.Infof("📊 Monitor | %d symbols × %d timeframes", len(u.config.Symbols), len(u.timeframes))

	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动倒计时显示协程
	go u.startCountdownDisplay(loopCtx)

	// 启动全局 15 分钟对齐强制“全量”刷新调度（对所有 timeframe）
	go func() {
		if err := u.runQuarterHourlyLoop(loopCtx); err != nil && !errors.Is(err, context.Canceled) {
			logrus.Errorf("Quarter scheduler error: %v", err)
		}
	}()

	// 首轮回填（按你现有逻辑；可选保留）
	for _, timeframe := range u.timeframes {
		logrus.Infof("🔄 Initial | [%s] starting initial update...", timeframe)
		success, failed, err := u.updateAllSymbolsForTimeframeWithType(loopCtx, timeframe, "initial")
		if err != nil {
			logrus.Errorf("❌ Initial | [%s] %v", timeframe, err)
		} else {
			logrus.Infof("✅ Initial | [%s] 🟢%d 🔴%d", timeframe, success, failed)
		}
	}

	errCh := make(chan error, len(u.timeframes))
	var wg sync.WaitGroup

	// 原有的“按收盘时间”循环（可选保留）
	for _, timeframe := range u.timeframes {
		tf := timeframe
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := u.runTimeframeLoop(loopCtx, tf); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errCh <- fmt.Errorf("timeframe %s: %w", tf, err):
				default:
				}
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
		runErr = ctx.Err()
	case err := <-errCh:
		runErr = err
	}

	cancel()
	wg.Wait()

	u.mu.Lock()
	u.isRunning = false
	u.mu.Unlock()

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return runErr
}

func (u *DataUpdater) runTimeframeLoop(ctx context.Context, timeframe string) error {
	baseInterval := u.resolveInterval(timeframe)
	currentInterval := baseInterval

	// 计算到下一个K线收盘时间的延迟
	firstDelay := timeUntilNextKlineClose(currentInterval)
	logrus.Infof("⏱️  [%s] first tick in %s (interval=%s, +5s delay)", timeframe, firstDelay.Truncate(time.Millisecond), currentInterval)

	timer := time.NewTimer(firstDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		// 确保不会过早更新，检查是否过了K线收盘时间
		nextBoundary := timeUntilNextBoundary(currentInterval)
		if nextBoundary > 10*time.Second {
			// 如果距离下次边界还有超过10秒，说明定时器提前触发了，等待到安全时间
			logrus.Warnf("⚠️  [%s] timer fired too early, waiting (remaining: %v)", timeframe, nextBoundary)
			timer.Reset(nextBoundary + 5*time.Second)
			continue
		}

		if err := u.runTimeframeCycle(ctx, timeframe); err != nil {
			logrus.Errorf("❌ Cycle   | [%s] %v", timeframe, err)
		}

		if u.config.AdaptiveInterval {
			newInterval := u.calculateOptimalInterval(currentInterval, baseInterval)
			if newInterval != currentInterval {
				currentInterval = newInterval
			}
		} else {
			currentInterval = baseInterval
		}

		// 计算到下一个K线收盘时间的延迟
		nextDelay := timeUntilNextKlineClose(currentInterval)
		logrus.Debugf("⏱️  [%s] next tick in %s (+5s delay)", timeframe, nextDelay.Truncate(time.Millisecond))
		timer.Reset(nextDelay)
	}
}

func (u *DataUpdater) runTimeframeCycle(ctx context.Context, timeframe string) error {
	loopStart := time.Now()

	stale, err := u.checkStaleData(ctx, timeframe)
	if err != nil {
		logrus.Errorf("[%s] stale check error: %v", timeframe, err)
	} else if len(stale) > 0 {
		logrus.Warnf("⚠️  Stale   | [%s] %d stale symbols, urgent updating...", timeframe, len(stale))
		u.urgentUpdate(ctx, timeframe, stale)
	}

	success, failed, err := u.UpdateAllSymbolsForTimeframe(ctx, timeframe)
	if err != nil {
		return err
	}

	logrus.Infof("🔄 Cycle   | [%s] ✅%d ❌%d ⏱️ %.1fs", timeframe, success, failed, time.Since(loopStart).Seconds())
	return nil
}

func (u *DataUpdater) UpdateAllSymbolsForTimeframe(ctx context.Context, timeframe string) (int, int, error) {
	return u.updateAllSymbolsForTimeframeWithType(ctx, timeframe, "regular")
}

func (u *DataUpdater) updateAllSymbolsForTimeframeWithType(ctx context.Context, timeframe string, updateType string) (int, int, error) {
	start := time.Now()
	logrus.Infof("📊 Batch   | [%s] %d symbols...", timeframe, len(u.config.Symbols))

	results := make(chan bool, len(u.config.Symbols))
	failedSymbols := make([]string, 0)
	var failedMu sync.Mutex

	batchSize := u.config.BatchSize
	if batchSize <= 0 {
		batchSize = u.config.ConcurrentRequests
	}
	if batchSize <= 0 {
		batchSize = len(u.config.Symbols)
	}
	if batchSize > len(u.config.Symbols) {
		batchSize = len(u.config.Symbols)
	}
	if u.config.ConcurrentRequests > 0 && batchSize > u.config.ConcurrentRequests {
		batchSize = u.config.ConcurrentRequests
	}

	totalBatches := 1
	if batchSize > 0 {
		totalBatches = (len(u.config.Symbols) + batchSize - 1) / batchSize
	}

	var wg sync.WaitGroup
	for i := 0; i < len(u.config.Symbols); i += batchSize {
		end := i + batchSize
		if end > len(u.config.Symbols) {
			end = len(u.config.Symbols)
		}
		batch := u.config.Symbols[i:end]
		logrus.Infof("📦 Batch   | [%s] %d/%d (%d symbols) %v", timeframe, i/batchSize+1, totalBatches, len(batch), batch)

		for _, symbol := range batch {
			sym := symbol
			wg.Add(1)
			go func() {
				defer wg.Done()
				// regular/initial 仍按原逻辑（增量优先），quarter 我们走强制“全量”
				forceFull := (updateType == quarterUpdateType)
				success := u.rateLimitedUpdate(ctx, sym, timeframe, forceFull)
				results <- success
				if !success {
					failedMu.Lock()
					failedSymbols = append(failedSymbols, sym)
					failedMu.Unlock()
				}
			}()
		}
	}

	wg.Wait()
	close(results)

	successCount, failureCount := 0, 0
	for r := range results {
		if r {
			successCount++
		} else {
			failureCount++
		}
	}

	elapsed := time.Since(start)

	// 更新统计
	u.stats.Lock()
	u.stats.successCount = successCount
	u.stats.failureCount = failureCount
	// 只在常规定时更新时更新 lastUpdate，避免初始化和紧急/quarter 更新干扰时间记录
	if updateType == "regular" {
		u.stats.lastUpdate = time.Now().In(cst)
	}
	u.stats.Unlock()

	// 更新系统状态
	avgFreshness := u.redisClient.CalculateAverageFreshness(ctx, u.config.Symbols, timeframe)
	status := &redisClient.SystemStatus{
		Timeframe:         timeframe,
		LastFullScan:      time.Now().In(cst).Format(TimeFormat),
		ActiveSymbols:     len(u.config.Symbols),
		SuccessfulUpdates: successCount,
		FailedUpdates:     failureCount,
		UpdateDuration:    elapsed.Seconds(),
		DataFreshnessAvg:  avgFreshness,
	}
	if err := u.redisClient.UpdateSystemStatus(ctx, timeframe, status); err != nil {
		logrus.Warnf("[%s] UpdateSystemStatus error: %v", timeframe, err)
	}

	if len(failedSymbols) > 0 {
		logrus.Warnf("[%s] Failed symbols: %v", timeframe, failedSymbols[:min(len(failedSymbols), 10)])
		if len(failedSymbols) > 10 {
			logrus.Warnf("[%s] ... and %d more", timeframe, len(failedSymbols)-10)
		}
	}

	return successCount, failureCount, nil
}

// NOTE: 新签名，支持强制全量
func (u *DataUpdater) rateLimitedUpdate(ctx context.Context, symbol, timeframe string, forceFull bool) bool {
	// 并发信号量
	select {
	case <-u.semaphore:
		defer func() { u.semaphore <- struct{}{} }()
	case <-ctx.Done():
		return false
	}

	// 速率限制
	if err := u.rateLimiter.Wait(ctx); err != nil {
		logrus.Errorf("%s [%s]: rate limit error: %v", symbol, timeframe, err)
		return false
	}

	return u.fetchAndStoreData(ctx, symbol, timeframe, forceFull, 3)
}

func (u *DataUpdater) fetchAndStoreData(ctx context.Context, symbol, timeframe string, forceFull bool, maxRetries int) bool {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		existing, err := u.getExistingData(ctx, symbol, timeframe)
		if err != nil {
			logrus.Warnf("%s [%s]: read existing data error: %v", symbol, timeframe, err)
		}

		if err := u.doFetchAndStore(ctx, symbol, timeframe, forceFull, existing); err != nil {
			logrus.Warnf("%s [%s]: attempt %d/%d failed: %v", symbol, timeframe, attempt, maxRetries, err)
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

func (u *DataUpdater) doFetchAndStore(ctx context.Context, symbol, timeframe string, forceFull bool, existing *redisClient.KlineData) error {
	interval := binance.ParseInterval(timeframe)

	if existing == nil || existing.CandleCount == 0 || existing.LastCandleTime == "" {
		logrus.Infof("🎆 Init    | %s [%s] fetching initial candles", symbol, timeframe)
		return u.doInitialFetch(ctx, symbol, timeframe)
	}

	// 分档容量：决定是否需要补齐（此处仅做提示，不触发 FIFO，保留为 full 判断依据）
	if cap := capacityForInterval(interval); existing.CandleCount < cap && !forceFull {
		logrus.Infof("🔄 Refill  | %s [%s] %d/%d refilling...", symbol, timeframe, existing.CandleCount, cap)
		return u.doFullUpdate(ctx, symbol, timeframe)
	}

	// 强制全量
	if forceFull {
		logrus.Infof("🟣 Force   | %s [%s] quarter tick -> FULL update", symbol, timeframe)
		return u.doFullUpdate(ctx, symbol, timeframe)
	}

	// 正常增量路径
	if u.canDoIncrementalUpdate(existing) {
		logrus.Infof("⚡ Incr    | %s [%s] %d candles (last=%s)", symbol, timeframe, existing.CandleCount, existing.LastCandleTime)
		if err := u.doIncrementalUpdate(ctx, symbol, timeframe, existing); err == nil {
			logrus.Infof("✅ Incr    | %s [%s] ok", symbol, timeframe)
			return nil
		} else {
			logrus.Warnf("❌ Incr    | %s [%s] %v -> fallback to full", symbol, timeframe, err)
		}
	} else {
		logrus.Infof("📋 Status  | %s [%s] cannot do incremental (candles=%d,last=%s)", symbol, timeframe, existing.CandleCount, existing.LastCandleTime)
	}

	return u.doFullUpdate(ctx, symbol, timeframe)
}

func (u *DataUpdater) canDoIncrementalUpdate(existing *redisClient.KlineData) bool {
	if existing == nil || existing.LastCandleTime == "" {
		return false
	}
	if existing.CandleCount < 1 {
		return false
	}
	_, err := time.Parse(TimeFormat, existing.LastCandleTime)
	return err == nil
}

func (u *DataUpdater) doIncrementalUpdate(ctx context.Context, symbol, timeframe string, existing *redisClient.KlineData) error {
	if existing == nil || existing.LastCandleTime == "" {
		return fmt.Errorf("missing existing candle metadata")
	}

	lastCandleTime, err := time.ParseInLocation(TimeFormat, existing.LastCandleTime, cst)
	if err != nil {
		return fmt.Errorf("invalid last candle time: %w", err)
	}

	interval := binance.ParseInterval(timeframe)
	if interval <= 0 {
		return fmt.Errorf("unsupported timeframe %s", timeframe)
	}

	now := time.Now().In(cst)
	lastClosed := lastClosedBoundary(now, interval)

	// 没有新“已收盘”
	if !lastClosed.After(lastCandleTime) {
		logrus.Debugf("⏰ Wait    | %s [%s] no closed new candle (last=%s, closed=%s)", symbol, timeframe, lastCandleTime.Format(TimeFormat), lastClosed.Format(TimeFormat))
		return nil
	}

	// 从下一根开盘开始拉
	start := lastCandleTime.Add(interval).UTC()
	limit := 200
	if interval >= time.Hour {
		limit = 500
	}

	klines, err := u.binanceClient.GetKlines(ctx, symbol, timeframe, limit, &start, nil)
	if err != nil {
		return fmt.Errorf("fetch incremental klines error: %w", err)
	}
	if len(klines) == 0 {
		logrus.Debugf("⚠️  No-op   | %s [%s] no klines returned from %s", symbol, timeframe, start.In(cst).Format(TimeFormat))
		return nil
	}

	// 仅保留“已收盘”的新根
	var newKlines []binance.KlineItem
	for _, k := range klines {
		kt := time.UnixMilli(k.OpenTime).In(cst)
		if kt.After(lastCandleTime) && !kt.After(lastClosed) {
			newKlines = append(newKlines, k)
		}
	}
	if len(newKlines) == 0 {
		logrus.Debugf("⚠️  No-op   | %s [%s] no closed candles after %s", symbol, timeframe, lastCandleTime.Format(TimeFormat))
		return nil
	}

	// 日线增量：不足 10 根 → 直接全量（一次 30 根更合算）
	if interval >= 24*time.Hour && len(newKlines) < 10 {
		logrus.Infof("🟨 IncrSkip| %s [%s] closed=%d < 10, fallback to FULL", symbol, timeframe, len(newKlines))
		return u.doFullUpdate(ctx, symbol, timeframe)
	}

	// 批量增量（不做 FIFO 剪裁）
	if err := u.storeKlineDataIncrementalBatch(ctx, symbol, timeframe, newKlines, existing); err != nil {
		return fmt.Errorf("store incremental error: %w", err)
	}

	latest := time.UnixMilli(newKlines[len(newKlines)-1].OpenTime).In(cst)
	logrus.Infof("🎆 New     | %s [%s] +%d (last=%s)", symbol, timeframe, len(newKlines), latest.Format(TimeFormat))
	return nil
}

// ---------- 分档策略 ----------

// 分档目标（用于 Completeness 的分母提示；不再用于 FIFO 裁剪）
// 1d=30, 4h=180, 1h=720；其它周期返回 0 表示不分档
func targetForInterval(interval time.Duration) (targetCandles int, targetDays int) {
	switch interval {
	case 24 * time.Hour: // 1d
		return 30, 30
	case 4 * time.Hour: // 4h
		return 180, 30
	case time.Hour: // 1h
		return 720, 30
	default:
		return 0, 0
	}
}

// 分档容量（保留给日志提示/回填判断，不用于实际裁剪）
func capacityForInterval(interval time.Duration) int {
	if tgt, _ := targetForInterval(interval); tgt > 0 {
		return tgt
	}
	return maxStoredCandles
}

// ---------- 数据拉取/存储 ----------

func (u *DataUpdater) doInitialFetch(ctx context.Context, symbol, timeframe string) error {
	// 初次拉取按分档容量限流，避免一次拿 1000 根对 1d 造成浪费
	capacity := capacityForInterval(binance.ParseInterval(timeframe))
	limit := initialKlineCount
	if capacity > 0 && capacity < limit {
		limit = capacity
	}

	klines, err := u.binanceClient.GetKlines(ctx, symbol, timeframe, limit, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch initial klines: %w", err)
	}
	if len(klines) == 0 {
		return fmt.Errorf("no initial kline data received")
	}
	if err := u.storeKlineData(symbol, timeframe, klines, nil, false, ctx); err != nil {
		return fmt.Errorf("failed to store initial data: %w", err)
	}
	logrus.Infof("✅ Init    | %s [%s] loaded %d candles", symbol, timeframe, len(klines))
	return nil
}

func (u *DataUpdater) doFullUpdate(ctx context.Context, symbol, timeframe string) error {
	interval := binance.ParseInterval(timeframe)

	// 分档：1d=30, 4h=180, 1h=720 → 优先 30 天覆盖
	targetCandles, targetDays := targetForInterval(interval)

	var dayOptions []int
	if targetDays > 0 {
		dayOptions = []int{max(u.config.DataDays, targetDays), 60, 45, 30, 15, 7}
	} else {
		dayOptions = []int{u.config.DataDays, 30, 15, 7}
	}

	for _, days := range dayOptions {
		if days <= 0 {
			continue
		}
		logrus.Debugf("%s [%s]: Attempting to fetch %d days of historical data", symbol, timeframe, days)

		klines, err := u.binanceClient.GetHistoricalData(ctx, symbol, timeframe, days)
		if err != nil {
			logrus.Warnf("%s [%s]: Failed to fetch %d days of data: %v", symbol, timeframe, days, err)
			continue
		}

		if len(klines) >= 10 {
			if err := u.storeKlineData(symbol, timeframe, klines, nil, false, ctx); err != nil {
				return fmt.Errorf("failed to store data: %w", err)
			}
			if targetCandles > 0 {
				logrus.Infof("✅ Full    | %s [%s] %d candles from %d days (target=%d)", symbol, timeframe, len(klines), days, targetCandles)
			} else {
				logrus.Infof("✅ Full    | %s [%s] %d candles from %d days", symbol, timeframe, len(klines))
			}
			return nil
		}

		logrus.Warnf("%s [%s]: Insufficient data for %d days: %d candles", symbol, timeframe, days, len(klines))
	}

	return fmt.Errorf("failed to fetch sufficient data with all day options")
}

func (u *DataUpdater) getExistingData(ctx context.Context, symbol, timeframe string) (*redisClient.KlineData, error) {
	data, err := u.redisClient.GetKlineData(ctx, symbol, timeframe)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	if data.CandleCount == 0 && data.OHLCVData == "" {
		return nil, nil
	}
	return data, nil
}

// storeKlineData：初始/全量写入；**不做 FIFO 裁剪**
func (u *DataUpdater) storeKlineData(symbol, timeframe string, klines []binance.KlineItem, existing *redisClient.KlineData, merge bool, ctx context.Context) error {
	if len(klines) == 0 {
		return fmt.Errorf("no kline data to store")
	}
	combined := make(map[string]map[string]string)

	if merge && existing != nil && existing.OHLCVData != "" {
		var existingMap map[string]interface{}
		if err := json.Unmarshal([]byte(existing.OHLCVData), &existingMap); err != nil {
			logrus.Warnf("%s [%s]: unmarshal existing error: %v", symbol, timeframe, err)
		} else {
			for ts, raw := range existingMap {
				switch m := raw.(type) {
				case map[string]interface{}:
					row := make(map[string]string, len(m))
					for k, v := range m {
						row[k] = fmt.Sprintf("%v", v)
					}
					combined[ts] = row
				case map[string]string:
					combined[ts] = m
				}
			}
		}
	}

	for _, kline := range klines {
		key := time.UnixMilli(kline.OpenTime).In(cst).Format(TimeFormat)
		combined[key] = map[string]string{
			"open":      kline.Open.String(),
			"high":      kline.High.String(),
			"low":       kline.Low.String(),
			"close":     kline.Close.String(),
			"volume":    kline.Volume.String(),
			"timestamp": strconv.FormatInt(kline.OpenTime, 10),
		}
	}

	return u.persistCleaned(ctx, symbol, timeframe, combined, existing)
}

// 单根增量（内部转批量）
func (u *DataUpdater) storeKlineDataIncremental(ctx context.Context, symbol, timeframe string, newKline binance.KlineItem, existing *redisClient.KlineData) error {
	return u.storeKlineDataIncrementalBatch(ctx, symbol, timeframe, []binance.KlineItem{newKline}, existing)
}

// 批量增量（**不做 FIFO 剪裁**）
func (u *DataUpdater) storeKlineDataIncrementalBatch(ctx context.Context, symbol, timeframe string, newKlines []binance.KlineItem, existing *redisClient.KlineData) error {
	existingMap := make(map[string]map[string]string)
	if existing != nil && existing.OHLCVData != "" {
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(existing.OHLCVData), &raw); err != nil {
			return fmt.Errorf("unmarshal existing OHLCV error: %w", err)
		}
		for ts, v := range raw {
			switch vv := v.(type) {
			case map[string]interface{}:
				row := make(map[string]string, len(vv))
				for k, val := range vv {
					row[k] = fmt.Sprintf("%v", val)
				}
				existingMap[ts] = row
			case map[string]string:
				existingMap[ts] = vv
			}
		}
	}

	for _, nk := range newKlines {
		key := time.UnixMilli(nk.OpenTime).In(cst).Format(TimeFormat)
		existingMap[key] = map[string]string{
			"open":      nk.Open.String(),
			"high":      nk.High.String(),
			"low":       nk.Low.String(),
			"close":     nk.Close.String(),
			"volume":    nk.Volume.String(),
			"timestamp": strconv.FormatInt(nk.OpenTime, 10),
		}
	}

	return u.persistCleaned(ctx, symbol, timeframe, existingMap, existing)
}

// 清洗+序列化+写入（**无 FIFO**），按“已收盘”过滤
func (u *DataUpdater) persistCleaned(ctx context.Context, symbol, timeframe string, data map[string]map[string]string, existing *redisClient.KlineData) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to persist")
	}

	interval := u.resolveInterval(timeframe)
	now := time.Now().In(cst)
	lastClosed := lastClosedBoundary(now, interval)

	// 过滤未来/未收盘
	timestamps := make([]time.Time, 0, len(data))
	clean := make(map[string]map[string]string, len(data))
	for ts, row := range data {
		t, err := time.ParseInLocation(TimeFormat, ts, cst)
		if err != nil {
			logrus.Warnf("%s [%s]: invalid time %s: %v", symbol, timeframe, ts, err)
			continue
		}
		if t.After(lastClosed) {
			continue
		}
		key := t.In(cst).Format(TimeFormat)
		clean[key] = row
		timestamps = append(timestamps, t.In(cst))
	}
	if len(timestamps) == 0 {
		return fmt.Errorf("no valid timestamps after clamping")
	}

	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i].Before(timestamps[j]) })

	startTime := timestamps[0]
	endTime := timestamps[len(timestamps)-1]

	// 序列化
	ohlcvMap := make(map[string]interface{}, len(clean))
	for ts, values := range clean {
		ohlcvMap[ts] = values
	}
	ohlcvData, err := json.Marshal(ohlcvMap)
	if err != nil {
		return fmt.Errorf("marshal OHLCV error: %w", err)
	}

	// 内容未变 → 跳过写入
	if existing != nil && existing.OHLCVData == string(ohlcvData) {
		logrus.Debugf("🟡 Skip    | %s [%s] ohlcv unchanged (%d candles), last=%s", symbol, timeframe, len(clean), endTime.Format(TimeFormat))
		return nil
	}

	// Completeness：不做容量 clamp；如果有分档目标，用目标作分母，否则用当前条数
	targetCandles, _ := targetForInterval(interval)
	expectedCandles := len(clean)
	if targetCandles > 0 && targetCandles > expectedCandles {
		expectedCandles = targetCandles
	}
	completeness := 1.0
	if expectedCandles > 0 {
		completeness = float64(len(clean)) / float64(expectedCandles)
		if completeness > 1 {
			completeness = 1
		}
	}

	klineData := &redisClient.KlineData{
		OHLCVData:      string(ohlcvData),
		LastCandleTime: endTime.In(cst).Format(TimeFormat),
		LastUpdate:     now.Format(TimeFormat),
		DataStart:      startTime.In(cst).Format(TimeFormat),
		DataEnd:        endTime.In(cst).Format(TimeFormat),
		Completeness:   completeness,
		CandleCount:    len(clean),
		Timeframe:      timeframe,
		DataAgeSeconds: now.Sub(endTime).Seconds(),
	}

	// 写入加 5s 超时，避免卡死
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return u.redisClient.StoreKlineData(writeCtx, symbol, timeframe, klineData)
}

// ---------- Quarter-hour 调度（全量刷新） ----------

// 到下一个 0/15/30/45 的延时；额外加 5 秒保证交易所数据可读
func timeUntilNextQuarterMark(now time.Time) time.Duration {
	q := 15 * time.Minute
	next := now.Truncate(q).Add(q)
	delay := next.Sub(now)
	if delay <= 0 {
		delay = q
	}
	return delay + 5*time.Second
}

// 统一的 quarter 标签
const quarterUpdateType = "quarter"

// 每逢 0/15/30/45 分，对所有 timeframe + 所有 symbol 强制跑“全量”写入
func (u *DataUpdater) runQuarterHourlyLoop(ctx context.Context) error {
	firstDelay := timeUntilNextQuarterMark(time.Now().In(cst))
	logrus.Infof("⏱️  [Quarter] first tick in %s (aligned to :00/:15/:30/:45)", firstDelay.Truncate(time.Millisecond))
	timer := time.NewTimer(firstDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			start := time.Now()
			logrus.Infof("🧭 Quarter | launching FULL refresh for all timeframes (%v)", u.timeframes)

			// 并发：以 timeframe 为单位并发；内部依旧遵守 limiter/semaphore
			var wg sync.WaitGroup
			successTotal, failedTotal := 0, 0
			var mu sync.Mutex

			for _, tf := range u.timeframes {
				tfLocal := tf
				wg.Add(1)
				go func() {
					defer wg.Done()
					// 这里传入 updateType=quarter，内部走 forceFull=true
					success, failed, err := u.updateAllSymbolsForTimeframeWithType(ctx, tfLocal, quarterUpdateType)
					mu.Lock()
					successTotal += success
					failedTotal += failed
					mu.Unlock()
					if err != nil {
						logrus.Errorf("❌ Quarter | [%s] %v", tfLocal, err)
					} else {
						logrus.Infof("✅ Quarter | [%s] ok (✅%d ❌%d)", tfLocal, success, failed)
					}
				}()
			}
			wg.Wait()

			logrus.Infof("🧭 Quarter | done in %.1fs (✅%d ❌%d)", time.Since(start).Seconds(), successTotal, failedTotal)

			nextDelay := timeUntilNextQuarterMark(time.Now().In(cst))
			timer.Reset(nextDelay)
		}
	}
}

// ---------- 其它 ----------

func (u *DataUpdater) checkStaleData(ctx context.Context, timeframe string) ([]string, error) {
	return u.redisClient.CheckDataFreshness(ctx, u.config.Symbols, timeframe, u.config.MaxDataAge)
}

func (u *DataUpdater) urgentUpdate(ctx context.Context, timeframe string, symbols []string) {
	if len(symbols) == 0 {
		return
	}
	logrus.Infof("[%s] urgent update: %v", timeframe, symbols)

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			u.rateLimitedUpdate(ctx, sym, timeframe, false)
		}(symbol)
	}
	wg.Wait()
}

func (u *DataUpdater) calculateOptimalInterval(currentInterval, minInterval time.Duration) time.Duration {
	currentHour := time.Now().In(cst).Hour()
	peakHours := []int{7, 8, 9, 13, 14, 15, 21, 22}
	isPeak := false
	for _, h := range peakHours {
		if currentHour == h {
			isPeak = true
			break
		}
	}

	base := currentInterval
	if base <= 0 {
		base = minInterval
	}

	if u.config.MarketHoursOptimization && !isPeak {
		base = time.Duration(float64(base) * u.config.OffPeakMultiplier)
	}

	u.stats.RLock()
	errRate := float64(u.stats.failureCount) / float64(u.stats.successCount+u.stats.failureCount)
	u.stats.RUnlock()

	if errRate > 0.2 {
		base = time.Duration(float64(base) * u.config.ErrorBackoffMultiplier)
	} else if errRate < 0.05 {
		base = time.Duration(float64(base) * u.config.SuccessRecoveryRate)
	}

	// clamp 到 [minInterval, 4*minInterval]
	if base < minInterval {
		base = minInterval
	}
	maxInt := 4 * minInterval
	if base > maxInt {
		base = maxInt
	}
	return base
}

func (u *DataUpdater) resolveInterval(timeframe string) time.Duration {
	interval := binance.ParseInterval(timeframe)
	if interval <= 0 {
		interval = time.Minute
	}
	return interval
}

// timeUntilNextKlineClose 计算到下一个K线收盘时间的延迟
// K线收盘时间就是下一个K线的开盘时间，再加5秒延迟确保数据可用
func timeUntilNextKlineClose(interval time.Duration) time.Duration {
	return timeUntilNextBoundary(interval) + 5*time.Second
}

func timeUntilNextBoundary(interval time.Duration) time.Duration {
	if interval <= 0 {
		return time.Second
	}
	now := time.Now().In(cst)
	var next time.Time

	switch interval {
	case time.Minute:
		next = now.Truncate(time.Minute).Add(time.Minute)
	case 3 * time.Minute:
		m := now.Minute()
		nm := ((m / 3) + 1) * 3
		if nm >= 60 {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), nm, 0, 0, now.Location())
		}
	case 5 * time.Minute:
		m := now.Minute()
		nm := ((m / 5) + 1) * 5
		if nm >= 60 {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), nm, 0, 0, now.Location())
		}
	case 15 * time.Minute:
		m := now.Minute()
		nm := ((m / 15) + 1) * 15
		if nm >= 60 {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), nm, 0, 0, now.Location())
		}
	case 30 * time.Minute:
		m := now.Minute()
		if m < 30 {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 30, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
		}
	case time.Hour:
		next = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
	case 2 * time.Hour:
		h := now.Hour()
		nh := ((h / 2) + 1) * 2
		if nh >= 24 {
			next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), nh, 0, 0, 0, now.Location())
		}
	case 4 * time.Hour:
		h := now.Hour()
		nh := ((h / 4) + 1) * 4
		if nh >= 24 {
			next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), nh, 0, 0, 0, now.Location())
		}
	case 6 * time.Hour:
		h := now.Hour()
		nh := ((h / 6) + 1) * 6
		if nh >= 24 {
			next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), nh, 0, 0, 0, now.Location())
		}
	case 8 * time.Hour:
		h := now.Hour()
		nh := ((h / 8) + 1) * 8
		if nh >= 24 {
			next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day(), nh, 0, 0, 0, now.Location())
		}
	case 12 * time.Hour:
		h := now.Hour()
		if h < 12 {
			next = time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		}
	case 24 * time.Hour:
		next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	default:
		next = now.Truncate(interval).Add(interval)
	}

	delay := next.Sub(now)
	if delay <= 0 {
		delay = interval
	}
	return delay
}

func lastClosedBoundary(now time.Time, interval time.Duration) time.Time {
	return now.Truncate(interval)
}

func (u *DataUpdater) GetResourceUsage() ResourceUsage {
	symbolCount := len(u.config.Symbols)
	totalCallsPerHour := 0.0
	for _, timeframe := range u.timeframes {
		interval := u.resolveInterval(timeframe)
		if interval <= 0 {
			continue
		}
		totalCallsPerHour += (3600.0 / interval.Seconds()) * float64(symbolCount)
	}
	estimatedMemory := symbolCount * len(u.timeframes) * 2
	if estimatedMemory == 0 {
		estimatedMemory = symbolCount * 2
	}
	return ResourceUsage{
		APICallsPerHour:    totalCallsPerHour,
		EstimatedMemoryMB:  estimatedMemory,
		BandwidthPerHourMB: (totalCallsPerHour * 10) / 1024,
	}
}

func (u *DataUpdater) HealthCheck(ctx context.Context) error {
	if err := u.redisClient.Ping(ctx); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}
	if err := u.binanceClient.TestConnectivity(ctx); err != nil {
		return fmt.Errorf("binance health check failed: %w", err)
	}
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

// Expose internal clients for fused services
func (u *DataUpdater) Redis() *redisClient.Client { return u.redisClient }
func (u *DataUpdater) Binance() *binance.Client { return u.binanceClient }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// startCountdownDisplay 启动倒计时显示协程
func (u *DataUpdater) startCountdownDisplay(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // 每10秒更新一次倒计时显示
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.displayCountdowns()
		}
	}
}

// displayCountdowns 显示所有时间周期的倒计时
func (u *DataUpdater) displayCountdowns() {
	now := time.Now().In(cst)
	var countdownInfos []string

	// 先显示全局 Quarter 刷新倒计时
	qDelay := timeUntilNextQuarterMark(now)
	countdownInfos = append(countdownInfos, formatCountdownDisplay("Quarter(全局15m)", qDelay))

	for _, timeframe := range u.timeframes {
		// 实时计算下次更新时间，而不是使用存储的时间
		interval := u.resolveInterval(timeframe)
		nextDelay := timeUntilNextKlineClose(interval)

		// 格式化单个倒计时显示
		formattedCountdown := formatCountdownDisplay(timeframe, nextDelay)
		countdownInfos = append(countdownInfos, formattedCountdown)
	}

	if len(countdownInfos) > 0 {
		// 创建美化的倒计时显示
		currentTime := now.In(cst).Format("15:04:05")
		header := fmt.Sprintf("⏰ K线更新倒计时 - %s", currentTime)

		logrus.Infof("")
		logrus.Infof("╭─────────────────────────────────────────────────────────────────────────╮")
		logrus.Infof("│ %-71s │", header)
		logrus.Infof("├─────────────────────────────────────────────────────────────────────────┤")

		// 按行显示，每行最多2个时间周期以确保对齐
		for i := 0; i < len(countdownInfos); i += 2 {
			end := i + 2
			if end > len(countdownInfos) {
				end = len(countdownInfos)
			}

			if end-i == 2 {
				// 两个项目的行
				logrus.Infof("│ %-34s │ %-34s │", countdownInfos[i], countdownInfos[end-1])
			} else {
				// 单个项目的行
				logrus.Infof("│ %-71s │", countdownInfos[i])
			}
		}

		logrus.Infof("╰─────────────────────────────────────────────────────────────────────────╯")
		logrus.Infof("")
	}
}

// formatCountdownDisplay 格式化单个倒计时显示，带颜色和特殊符号
func formatCountdownDisplay(timeframe string, remaining time.Duration) string {
	timeStr := formatDuration(remaining)

	// 根据剩余时间选择不同的图标和样式
	var icon, status string
	totalSeconds := int(remaining.Seconds())

	switch {
	case totalSeconds <= 60: // 1分钟内
		icon = "🔥"
		status = "即将更新"
	case totalSeconds <= 300: // 5分钟内
		icon = "⚡"
		status = "准备中"
	case totalSeconds <= 1800: // 30分钟内
		icon = "🕐"
		status = "等待中"
	default:
		icon = "⏱️"
		status = "待机中"
	}

	// 美化时间周期显示
	var tfDisplay string
	switch timeframe {
	case "1m":
		tfDisplay = "1分钟"
	case "5m":
		tfDisplay = "5分钟"
	case "15m":
		tfDisplay = "15分钟"
	case "30m":
		tfDisplay = "30分钟"
	case "1h":
		tfDisplay = "1小时"
	case "4h":
		tfDisplay = "4小时"
	case "1d":
		tfDisplay = "1天"
	default:
		tfDisplay = timeframe
	}

	return fmt.Sprintf("%s %s %s %s", icon, tfDisplay, timeStr, status)
}

// formatDuration 格式化时间间隔为易读格式
func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "00:00"
	}

	totalSeconds := int(d.Seconds())
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	if hours > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

// GetCountdowns 获取所有时间周期的倒计时信息
func (u *DataUpdater) GetCountdowns() map[string]string {
	result := make(map[string]string)

	// 全局 quarter
	result["quarter"] = formatDuration(timeUntilNextQuarterMark(time.Now().In(cst)))

	for _, timeframe := range u.timeframes {
		// 实时计算下次更新时间
		interval := u.resolveInterval(timeframe)
		nextDelay := timeUntilNextKlineClose(interval)
		result[timeframe] = formatDuration(nextDelay)
	}

	return result
}

func (u *DataUpdater) GetStats() map[string]interface{} {
	u.stats.RLock()
	defer u.stats.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	lastUpd := ""
	if !u.stats.lastUpdate.IsZero() {
		lastUpd = u.stats.lastUpdate.In(cst).Format(TimeFormat)
	}

	return map[string]interface{}{
		"success_count": u.stats.successCount,
		"failure_count": u.stats.failureCount,
		"last_update":   lastUpd,
		"memory_mb":     memStats.Alloc / 1024 / 1024,
		"goroutines":    runtime.NumGoroutine(),
		"is_running":    u.isRunning,
		"timeframes":    append([]string(nil), u.timeframes...),
		"countdowns":    u.GetCountdowns(),
	}
}
