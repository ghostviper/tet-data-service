package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// Binance API配置
	BinanceBaseURL string

	// Redis配置
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// 更新配置
	UpdateInterval     time.Duration
	ConcurrentRequests int
	MaxDataAge         time.Duration
	APIRequestsPerSec  int
	IncrementalCandles int
	Timeframe          string
	DataDays           int

	// 交易对列表
	Symbols []string

	// 批次配置
	BatchSize int

	// 性能阈值
	MaxUpdateTimeSeconds int
	MaxFailureRate       float64
	MemoryLimitMB        int

	// 自适应参数
	AdaptiveInterval       bool
	ErrorBackoffMultiplier float64
	SuccessRecoveryRate    float64
	MarketHoursOptimization bool
	OffPeakMultiplier      float64
}

func Load(configPath string) (*Config, error) {
	// 加载.env文件（如果存在）
	if _, err := os.Stat(configPath); err == nil {
		if err := godotenv.Load(configPath); err != nil {
			return nil, fmt.Errorf("error loading .env file: %w", err)
		}
	}

	cfg := &Config{
		// 默认值
		BinanceBaseURL:     getEnv("BINANCE_BASE_URL", "https://api.binance.com"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvInt("REDIS_DB", 1),

		UpdateInterval:     time.Duration(getEnvInt("UPDATE_INTERVAL", 180)) * time.Second,
		ConcurrentRequests: getEnvInt("CONCURRENT_REQUESTS", 12),
		MaxDataAge:         time.Duration(getEnvInt("MAX_DATA_AGE", 540)) * time.Second,
		APIRequestsPerSec:  getEnvInt("API_REQUESTS_PER_SEC", 15),
		IncrementalCandles: getEnvInt("INCREMENTAL_CANDLES", 6),
		Timeframe:          getEnv("TIMEFRAME", "15m"),
		DataDays:           getEnvInt("DATA_DAYS", 30),
		BatchSize:          getEnvInt("BATCH_SIZE", 15),

		MaxUpdateTimeSeconds: getEnvInt("MAX_UPDATE_TIME", 30),
		MaxFailureRate:       getEnvFloat("MAX_FAILURE_RATE", 0.1),
		MemoryLimitMB:        getEnvInt("MEMORY_LIMIT_MB", 500),

		AdaptiveInterval:         getEnvBool("ADAPTIVE_INTERVAL", true),
		ErrorBackoffMultiplier:   getEnvFloat("ERROR_BACKOFF_MULTIPLIER", 1.5),
		SuccessRecoveryRate:      getEnvFloat("SUCCESS_RECOVERY_RATE", 0.9),
		MarketHoursOptimization:  getEnvBool("MARKET_HOURS_OPTIMIZATION", true),
		OffPeakMultiplier:        getEnvFloat("OFF_PEAK_MULTIPLIER", 2.0),
	}

	// 加载交易对列表
	symbolsStr := getEnv("SYMBOLS", "")
	if symbolsStr != "" {
		cfg.Symbols = strings.Split(symbolsStr, ",")
		// 清理空白字符
		for i, symbol := range cfg.Symbols {
			cfg.Symbols[i] = strings.TrimSpace(symbol)
		}
	} else {
		// 使用默认交易对列表（与Python版本保持一致）
		cfg.Symbols = []string{
			"BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
			"SOL/USDT", "DOGE/USDT", "DOT/USDT", "UNI/USDT", "LINK/USDT",
			"AVAX/USDT", "MEME/USDT", "PUMP/USDT", "PEOPLE/USDT", "BOME/USDT",
			"ARB/USDT", "MATIC/USDT", "FTM/USDT", "SAND/USDT", "AXS/USDT",
			"NEAR/USDT", "ALGO/USDT", "XLM/USDT", "LIT/USDT", "MKR/USDT",
		}
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}
