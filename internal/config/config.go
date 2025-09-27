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
	ConcurrentRequests int
	MaxDataAge         time.Duration
	APIRequestsPerSec  int
	IncrementalCandles int
	Timeframe          string
	Timeframes         []string
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
	AdaptiveInterval        bool
	ErrorBackoffMultiplier  float64
	SuccessRecoveryRate     float64
	MarketHoursOptimization bool
	OffPeakMultiplier       float64

	// Fused services toggles (from .env)
	OrderbookEnabled     bool
	OrderbookKeepHours   int
	LiquidationEnabled   bool
	LiquidationKeepHours int
	OIEnabled            bool
	OIPeriod             string
	OIIntervalSec        int

	// Volatility monitor
	VolatilityEnabled     bool
	VolatilityTimeframe   string
	VolatilityIntervalSec int
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
		BinanceBaseURL: getEnv("BINANCE_BASE_URL", "https://fapi.binance.com"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  getEnv("REDIS_PASSWORD", ""),
		RedisDB:        getEnvInt("REDIS_DB", 1),

		ConcurrentRequests: getEnvInt("CONCURRENT_REQUESTS", 12),
		MaxDataAge:         time.Duration(getEnvInt("MAX_DATA_AGE", 540)) * time.Second,
		APIRequestsPerSec:  getEnvInt("API_REQUESTS_PER_SEC", 15),
		IncrementalCandles: getEnvInt("INCREMENTAL_CANDLES", 1),
		Timeframe:          getEnv("TIMEFRAME", "15m"),
		DataDays:           getEnvInt("DATA_DAYS", 30),
		BatchSize:          getEnvInt("BATCH_SIZE", 15),

		MaxUpdateTimeSeconds: getEnvInt("MAX_UPDATE_TIME", 30),
		MaxFailureRate:       getEnvFloat("MAX_FAILURE_RATE", 0.1),
		MemoryLimitMB:        getEnvInt("MEMORY_LIMIT_MB", 500),

		AdaptiveInterval:        getEnvBool("ADAPTIVE_INTERVAL", true),
		ErrorBackoffMultiplier:  getEnvFloat("ERROR_BACKOFF_MULTIPLIER", 1.5),
		SuccessRecoveryRate:     getEnvFloat("SUCCESS_RECOVERY_RATE", 0.9),
		MarketHoursOptimization: getEnvBool("MARKET_HOURS_OPTIMIZATION", true),
		OffPeakMultiplier:       getEnvFloat("OFF_PEAK_MULTIPLIER", 2.0),

		// Fused services toggles
		OrderbookEnabled:     getEnvBool("ORDERBOOK_ENABLED", true),
		OrderbookKeepHours:   getEnvInt("ORDERBOOK_KEEP_HOURS", 2),
		LiquidationEnabled:   getEnvBool("LIQUIDATION_ENABLED", true),
		LiquidationKeepHours: getEnvInt("LIQUIDATION_KEEP_HOURS", 24),
		OIEnabled:            getEnvBool("OI_ENABLED", false),
		OIPeriod:             getEnv("OI_PERIOD", "5m"),
		OIIntervalSec:        getEnvInt("OI_INTERVAL", 300),

		// Volatility
		VolatilityEnabled:     getEnvBool("VOLATILITY_ENABLED", false),
		VolatilityTimeframe:   getEnv("VOLATILITY_TIMEFRAME", "1h"),
		VolatilityIntervalSec: getEnvInt("VOLATILITY_INTERVAL", 300),
	}

	defaultSymbols := []string{
		"BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT",
		"ADA/USDT", "SOL/USDT", "DOGE/USDT", "DOT/USDT",
		"UNI/USDT", "LINK/USDT", "AVAX/USDT", "MEME/USDT",
		"PUMP/USDT", "PEOPLE/USDT", "BOME/USDT", "ARB/USDT",
		"FTM/USDT", "AXS/USDT", "SAND/USDT", "ENA/USDT",
		"NEAR/USDT", "ALGO/USDT", "XLM/USDT", "AAVE/USDT",
		"FIL/USDT", "ETHFI/USDT", "LINEA/USDT",
		"LIT/USDT", "MKR/USDT",
		"SUSHI/USDT", "CRV/USDT",
		"PENGU/USDT", "SUI/USDT", "TRX/USDT", "WIF/USDT",
		"HBAR/USDT", "APT/USDT", "SEI/USDT", "LDO/USDT", "TAO/USDT",
		"TON/USDT", "TIA/USDT", "INJ/USDT",
		"ONDO/USDT", "FET/USDT", "NMR/USDT", "ETC/USDT",
		"BCH/USDT", "LTC/USDT", "XMR/USDT", "ZEC/USDT",
		"1000SHIB/USDT", "1000PEPE/USDT", "PUMP/USDT",
		"1000BONK/USDT", "IP/USDT",
	}
	if customDefaults := strings.TrimSpace(getEnv("DEFAULT_SYMBOLS", "")); customDefaults != "" {
		defaultSymbols = parseList(customDefaults)
	}

	symbolsStr := strings.TrimSpace(getEnv("SYMBOLS", ""))
	if symbolsStr != "" {
		cfg.Symbols = parseList(symbolsStr)
	} else {
		cfg.Symbols = append([]string(nil), defaultSymbols...)
	}

	if additional := strings.TrimSpace(getEnv("SYMBOLS_APPEND", "")); additional != "" {
		cfg.Symbols = append(cfg.Symbols, parseList(additional)...)
	}

	// 加载时间周期列表
	timeframesStr := getEnv("TIMEFRAMES", "")
	if timeframesStr != "" {
		cfg.Timeframes = parseList(timeframesStr)
		if cfg.Timeframe == "" && len(cfg.Timeframes) > 0 {
			cfg.Timeframe = cfg.Timeframes[0]
		}
	} else {
		cfg.Timeframes = []string{cfg.Timeframe}
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

func parseList(input string) []string {
	parts := strings.Split(input, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		clean := strings.TrimSpace(part)
		if clean != "" {
			result = append(result, clean)
		}
	}
	return result
}
