package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/shopspring/decimal"
	"golang.org/x/time/rate"
)

// 配置结构
type Config struct {
	BinanceBaseURL string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	UpdateInterval     time.Duration
	ConcurrentRequests int
	MaxDataAge         time.Duration
	APIRequestsPerSec  int
	Timeframe          string
	DataDays           int
	BatchSize          int

	Symbols []string
}

// Binance API 相关结构
type KlineItem struct {
	OpenTime  int64
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal
	Volume    decimal.Decimal
	CloseTime int64
}

type KlineResponse [][]interface{}

// Redis 存储结构
type KlineData struct {
	OHLCVData      string  `json:"ohlcv_data"`
	LastCandleTime string  `json:"last_candle_time"`
	LastUpdate     string  `json:"last_update"`
	DataStart      string  `json:"data_start"`
	DataEnd        string  `json:"data_end"`
	Completeness   float64 `json:"data_completeness"`
	CandleCount    int     `json:"candle_count"`
	Timeframe      string  `json:"timeframe"`
	DataAgeSeconds float64 `json:"data_age_seconds"`
}

type SystemStatus struct {
	LastFullScan        string  `json:"last_full_scan"`
	ActiveSymbols       int     `json:"active_symbols"`
	SuccessfulUpdates   int     `json:"successful_updates"`
	FailedUpdates       int     `json:"failed_updates"`
	UpdateDuration      float64 `json:"update_duration_seconds"`
	DataFreshnessAvg    float64 `json:"data_freshness_avg"`
}

// 主服务结构
type DataService struct {
	config      *Config
	httpClient  *http.Client
	redisClient *redis.Client
	rateLimiter *rate.Limiter
	semaphore   chan struct{}
	isRunning   bool
	mu          sync.RWMutex
	stats       struct {
		sync.RWMutex
		successCount int
		failureCount int
	}
}

func main() {
	// 命令行参数
	var (
		redisAddr    = flag.String("redis", "localhost:6379", "Redis address")
		redisDB      = flag.Int("redis-db", 1, "Redis database")
		interval     = flag.Int("interval", 180, "Update interval in seconds")
		concurrent   = flag.Int("concurrent", 12, "Concurrent requests")
		maxAge       = flag.Int("max-age", 540, "Max data age in seconds")
		timeframe    = flag.String("timeframe", "15m", "Kline timeframe")
		symbolsFlag  = flag.String("symbols", "", "Comma-separated symbols (optional)")
		configFile   = flag.String("config", "", "Config file path (optional)")
	)
	flag.Parse()

	// 加载配置
	cfg := loadConfig(*configFile, *redisAddr, *redisDB, *interval, *concurrent, *maxAge, *timeframe, *symbolsFlag)

	// 创建服务
	service := NewDataService(cfg)
	defer service.Close()

	// 显示配置信息
	log.Printf("TET Data Service Starting...")
	log.Printf("Config: interval=%v, concurrent=%d, max-age=%v, timeframe=%s",
		cfg.UpdateInterval, cfg.ConcurrentRequests, cfg.MaxDataAge, cfg.Timeframe)
	log.Printf("Monitoring %d symbols", len(cfg.Symbols))

	// 健康检查
	ctx := context.Background()
	if err := service.HealthCheck(ctx); err != nil {
		log.Printf("Warning - Health check failed: %v", err)
	} else {
		log.Printf("Health check passed")
	}

	// 资源使用估算
	usage := service.GetResourceUsage()
	log.Printf("Resource estimation: %.0f API calls/hour, %dMB memory, %.1fMB/hour bandwidth",
		usage.APICallsPerHour, usage.EstimatedMemoryMB, usage.BandwidthPerHourMB)

	// 设置优雅关闭
	ctx, cancel := context.WithCancel(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// 启动服务
	if err := service.Start(ctx); err != nil && err != context.Canceled {
		log.Printf("Service stopped with error: %v", err)
		os.Exit(1)
	}

	log.Printf("TET Data Service stopped")
}

func loadConfig(configFile, redisAddr string, redisDB, interval, concurrent, maxAge int, timeframe, symbolsFlag string) *Config {
	_ = configFile
	cfg := &Config{
		BinanceBaseURL:     "https://api.binance.com",
		UpdateInterval:     time.Duration(interval) * time.Second,
		ConcurrentRequests: concurrent,
		MaxDataAge:         time.Duration(maxAge) * time.Second,
		APIRequestsPerSec:  15,
		Timeframe:          timeframe,
		DataDays:           30,
		BatchSize:          15,
		RedisAddr:          redisAddr,
		RedisDB:            redisDB,
	}

	cfg.RedisPassword = os.Getenv("REDIS_PASSWORD")

	// 解析交易对
	if symbolsFlag != "" {
		cfg.Symbols = strings.Split(symbolsFlag, ",")
		for i, symbol := range cfg.Symbols {
			cfg.Symbols[i] = strings.TrimSpace(symbol)
		}
	} else {
		// 默认交易对
		cfg.Symbols = []string{
			"BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
			"SOL/USDT", "DOGE/USDT", "DOT/USDT", "UNI/USDT", "LINK/USDT",
			"AVAX/USDT", "MEME/USDT", "PUMP/USDT", "PEOPLE/USDT", "BOME/USDT",
			"ARB/USDT", "MATIC/USDT", "FTM/USDT", "SAND/USDT", "AXS/USDT",
			"NEAR/USDT", "ALGO/USDT", "XLM/USDT", "LIT/USDT", "MKR/USDT",
		}
	}

	return cfg
}

func NewDataService(cfg *Config) *DataService {
	// 创建信号量
	semaphore := make(chan struct{}, cfg.ConcurrentRequests)
	for i := 0; i < cfg.ConcurrentRequests; i++ {
		semaphore <- struct{}{}
	}

	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	return &DataService{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		redisClient: rdb,
		rateLimiter: rate.NewLimiter(rate.Limit(cfg.APIRequestsPerSec), cfg.APIRequestsPerSec),
		semaphore:   semaphore,
	}
}

func (s *DataService) Start(ctx context.Context) error {
	s.mu.Lock()
	s.isRunning = true
	s.mu.Unlock()

	log.Printf("TET Data Service started")

	// 首次完整更新
	log.Printf("Performing initial full data update...")
	successCount, failureCount := s.UpdateAllSymbols(ctx)
	log.Printf("Initial update completed: %d success, %d failed", successCount, failureCount)

	// 主循环
	ticker := time.NewTicker(s.config.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, stopping data service")
			s.mu.Lock()
			s.isRunning = false
			s.mu.Unlock()
			return ctx.Err()

		case <-ticker.C:
			loopStart := time.Now()

			// 检查过期数据
			staleSymbols := s.checkStaleData(ctx)
			if len(staleSymbols) > 0 {
				log.Printf("Found %d stale symbols, updating urgently", len(staleSymbols))
				s.urgentUpdate(ctx, staleSymbols)
			}

			// 常规更新
			if s.isRunning {
				successCount, failureCount := s.UpdateAllSymbols(ctx)
				log.Printf("Update completed: %d success, %d failed, duration: %.1fs",
					successCount, failureCount, time.Since(loopStart).Seconds())
			}
		}
	}
}

func (s *DataService) UpdateAllSymbols(ctx context.Context) (int, int) {
	start := time.Now()
	log.Printf("Starting batch update for %d symbols", len(s.config.Symbols))

	var wg sync.WaitGroup
	results := make(chan bool, len(s.config.Symbols))
	failedSymbols := make([]string, 0)
	var failedMu sync.Mutex

	// 分批处理
	batchSize := s.config.BatchSize
	for i := 0; i < len(s.config.Symbols); i += batchSize {
		end := i + batchSize
		if end > len(s.config.Symbols) {
			end = len(s.config.Symbols)
		}

		batch := s.config.Symbols[i:end]
		log.Printf("Processing batch %d: %v", i/batchSize+1, batch[:min(len(batch), 5)])

		// 批次内并发处理
		for _, symbol := range batch {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				success := s.rateLimitedUpdate(ctx, sym)
				results <- success
				if !success {
					failedMu.Lock()
					failedSymbols = append(failedSymbols, sym)
					failedMu.Unlock()
				}
			}(symbol)
		}

		wg.Wait()

		// 批次间延迟
		if end < len(s.config.Symbols) {
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

	// 更新统计
	s.stats.Lock()
	s.stats.successCount = successCount
	s.stats.failureCount = failureCount
	s.stats.Unlock()

	// 更新系统状态
	avgFreshness := s.calculateAverageFreshness(ctx)
	status := &SystemStatus{
		LastFullScan:        time.Now().Format(time.RFC3339),
		ActiveSymbols:       len(s.config.Symbols),
		SuccessfulUpdates:   successCount,
		FailedUpdates:       failureCount,
		UpdateDuration:      elapsed.Seconds(),
		DataFreshnessAvg:    avgFreshness,
	}

	s.updateSystemStatus(ctx, status)

	if len(failedSymbols) > 0 {
		log.Printf("Failed symbols: %v", failedSymbols[:min(len(failedSymbols), 10)])
		if len(failedSymbols) > 10 {
			log.Printf("... and %d more", len(failedSymbols)-10)
		}
	}

	return successCount, failureCount
}

func (s *DataService) rateLimitedUpdate(ctx context.Context, symbol string) bool {
	// 获取信号量
	select {
	case <-s.semaphore:
		defer func() { s.semaphore <- struct{}{} }()
	case <-ctx.Done():
		return false
	}

	// 速率限制
	if err := s.rateLimiter.Wait(ctx); err != nil {
		log.Printf("%s: Rate limit error: %v", symbol, err)
		return false
	}

	return s.fetchAndStoreData(ctx, symbol)
}

func (s *DataService) fetchAndStoreData(ctx context.Context, symbol string) bool {
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// 尝试获取历史数据
		klines, err := s.getHistoricalData(ctx, symbol, s.config.Timeframe, s.config.DataDays)
		if err != nil {
			log.Printf("%s: Attempt %d/%d failed: %v", symbol, attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return false
		}

		if len(klines) < 10 {
			log.Printf("%s: Insufficient data: %d candles", symbol, len(klines))
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return false
		}

		// 存储数据
		if err := s.storeKlineData(ctx, symbol, klines); err != nil {
			log.Printf("%s: Store failed: %v", symbol, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return false
		}

		log.Printf("%s: Updated successfully with %d candles", symbol, len(klines))
		return true
	}
	return false
}

func (s *DataService) getHistoricalData(ctx context.Context, symbol, interval string, days int) ([]KlineItem, error) {
	// 转换交易对格式 BTC/USDT -> BTCUSDT
	binanceSymbol := strings.ReplaceAll(symbol, "/", "")

	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -days)

	params := url.Values{}
	params.Set("symbol", binanceSymbol)
	params.Set("interval", interval)
	params.Set("startTime", strconv.FormatInt(startTime.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(endTime.UnixMilli(), 10))
	params.Set("limit", "1000")

	data, err := s.sendRequest(ctx, "/api/v3/klines", params)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}

	var response KlineResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("JSON unmarshal failed: %w", err)
	}

	return s.parseKlineResponse(response)
}

func (s *DataService) sendRequest(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	fullURL := s.config.BinanceBaseURL + endpoint + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	return body, nil
}

func (s *DataService) parseKlineResponse(response KlineResponse) ([]KlineItem, error) {
	var klines []KlineItem

	for _, item := range response {
		if len(item) < 6 {
			continue
		}

		openTime := int64(item[0].(float64))
		closeTime := int64(item[6].(float64))

		open, _ := decimal.NewFromString(item[1].(string))
		high, _ := decimal.NewFromString(item[2].(string))
		low, _ := decimal.NewFromString(item[3].(string))
		close, _ := decimal.NewFromString(item[4].(string))
		volume, _ := decimal.NewFromString(item[5].(string))

		kline := KlineItem{
			OpenTime:  openTime,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			CloseTime: closeTime,
		}

		klines = append(klines, kline)
	}

	return klines, nil
}

func (s *DataService) storeKlineData(ctx context.Context, symbol string, klines []KlineItem) error {
	if len(klines) == 0 {
		return fmt.Errorf("no data to store")
	}

	// 转换为JSON格式
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
		return err
	}

	// 构建存储数据
	now := time.Now()
	lastKline := klines[len(klines)-1]
	dataEndTime := time.UnixMilli(lastKline.CloseTime)
	dataAge := now.Sub(dataEndTime).Seconds()

	klineData := &KlineData{
		OHLCVData:      string(ohlcvData),
		LastCandleTime: dataEndTime.Format(time.RFC3339),
		LastUpdate:     now.Format(time.RFC3339),
		DataStart:      time.UnixMilli(klines[0].OpenTime).Format(time.RFC3339),
		DataEnd:        dataEndTime.Format(time.RFC3339),
		Completeness:   float64(len(klines)) / (float64(s.config.DataDays) * 24 * 4),
		CandleCount:    len(klines),
		Timeframe:      s.config.Timeframe,
		DataAgeSeconds: dataAge,
	}

	jsonData, err := json.Marshal(klineData)
	if err != nil {
		return err
	}

	// Redis键名
	key := fmt.Sprintf("tet:kline:%s:%s", s.config.Timeframe, strings.ReplaceAll(symbol, "/", "_"))
	timestampKey := fmt.Sprintf("tet:timestamp:%s:%s", s.config.Timeframe, strings.ReplaceAll(symbol, "/", "_"))

	// 存储到Redis
	pipe := s.redisClient.Pipeline()
	pipe.SetEX(ctx, key, jsonData, 24*time.Hour)
	pipe.SetEX(ctx, timestampKey, now.Format(time.RFC3339), 2*time.Hour)
	_, err = pipe.Exec(ctx)

	return err
}

func (s *DataService) checkStaleData(ctx context.Context) []string {
	var staleSymbols []string
	now := time.Now()

	for _, symbol := range s.config.Symbols {
		key := fmt.Sprintf("tet:timestamp:%s:%s", s.config.Timeframe, strings.ReplaceAll(symbol, "/", "_"))
		result, err := s.redisClient.Get(ctx, key).Result()

		if err != nil {
			staleSymbols = append(staleSymbols, symbol)
			continue
		}

		lastUpdate, err := time.Parse(time.RFC3339, result)
		if err != nil || now.Sub(lastUpdate) > s.config.MaxDataAge {
			staleSymbols = append(staleSymbols, symbol)
		}
	}

	return staleSymbols
}

func (s *DataService) urgentUpdate(ctx context.Context, symbols []string) {
	var wg sync.WaitGroup
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			s.rateLimitedUpdate(ctx, sym)
		}(symbol)
	}
	wg.Wait()
}

func (s *DataService) calculateAverageFreshness(ctx context.Context) float64 {
	now := time.Now()
	var totalAge float64
	var validCount int

	for _, symbol := range s.config.Symbols {
		key := fmt.Sprintf("tet:timestamp:%s:%s", s.config.Timeframe, strings.ReplaceAll(symbol, "/", "_"))
		result, err := s.redisClient.Get(ctx, key).Result()

		if err != nil {
			continue
		}

		lastUpdate, err := time.Parse(time.RFC3339, result)
		if err != nil {
			continue
		}

		age := now.Sub(lastUpdate).Seconds()
		totalAge += age
		validCount++
	}

	if validCount == 0 {
		return float64(s.config.MaxDataAge.Seconds())
	}

	return totalAge / float64(validCount)
}

func (s *DataService) updateSystemStatus(ctx context.Context, status *SystemStatus) {
	jsonData, err := json.Marshal(status)
	if err != nil {
		return
	}

	key := "tet:system:status"
	s.redisClient.SetEX(ctx, key, jsonData, 2*time.Hour)
}

type ResourceUsage struct {
	APICallsPerHour     float64
	EstimatedMemoryMB   int
	BandwidthPerHourMB  float64
}

func (s *DataService) GetResourceUsage() ResourceUsage {
	symbolCount := len(s.config.Symbols)
	callsPerHour := (3600.0 / s.config.UpdateInterval.Seconds()) * float64(symbolCount)

	return ResourceUsage{
		APICallsPerHour:     callsPerHour,
		EstimatedMemoryMB:   symbolCount * 2,
		BandwidthPerHourMB:  (callsPerHour * 10) / 1024,
	}
}

func (s *DataService) HealthCheck(ctx context.Context) error {
	// 测试Redis连接
	if err := s.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis connection failed: %w", err)
	}

	// 测试Binance连接
	_, err := s.sendRequest(ctx, "/api/v3/ping", url.Values{})
	if err != nil {
		return fmt.Errorf("binance connection failed: %w", err)
	}

	return nil
}

func (s *DataService) Close() error {
	s.mu.Lock()
	s.isRunning = false
	s.mu.Unlock()

	return s.redisClient.Close()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
