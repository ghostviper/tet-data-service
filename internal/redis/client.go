package redis

import (
    "context"
    "encoding/json"
    "fmt"
    "math"
    "strings"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/sirupsen/logrus"
)

// 时间格式常量（不带 Z 后缀的 ISO 8601 格式）
const TimeFormat = "2006-01-02T15:04:05.000"

type Client struct {
	rdb *redis.Client
}

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
	Timeframe         string  `json:"timeframe"`
	LastFullScan      string  `json:"last_full_scan"`
	ActiveSymbols     int     `json:"active_symbols"`
	SuccessfulUpdates int     `json:"successful_updates"`
	FailedUpdates     int     `json:"failed_updates"`
	UpdateDuration    float64 `json:"update_duration_seconds"`
	DataFreshnessAvg  float64 `json:"data_freshness_avg"`
}

func NewClient(addr, password string, db int) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &Client{rdb: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.rdb.Ping(ctx).Result()
	return err
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

// Redis键名生成方法
func (c *Client) GetKlineKey(symbol, timeframe string) string {
	return fmt.Sprintf("tet:kline:%s:%s", timeframe, normalizeSymbol(symbol))
}

func (c *Client) GetTimestampKey(symbol, timeframe string) string {
	return fmt.Sprintf("tet:timestamp:%s:%s", timeframe, normalizeSymbol(symbol))
}

func (c *Client) GetSystemStatusKey(timeframe string) string {
	if timeframe == "" {
		return "tet:system:status"
	}
	return fmt.Sprintf("tet:system:status:%s", timeframe)
}

func normalizeSymbol(symbol string) string {
	return strings.ReplaceAll(symbol, "/", "_")
}

// 存储K线数据
func (c *Client) StoreKlineData(ctx context.Context, symbol, timeframe string, data *KlineData) error {
	key := c.GetKlineKey(symbol, timeframe)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal kline data: %w", err)
	}

	// 存储数据，24小时过期
	if err := c.rdb.SetEX(ctx, key, jsonData, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to store kline data: %w", err)
	}

	// 更新时间戳
	timestampKey := c.GetTimestampKey(symbol, timeframe)
	now := time.Now().In(time.FixedZone("CST", 8*3600)).Format(TimeFormat)
	if err := c.rdb.SetEX(ctx, timestampKey, now, 2*time.Hour).Err(); err != nil {
		logrus.Warnf("Failed to update timestamp for %s: %v", symbol, err)
	}

	return nil
}

// 获取K线数据
func (c *Client) GetKlineData(ctx context.Context, symbol, timeframe string) (*KlineData, error) {
	key := c.GetKlineKey(symbol, timeframe)

	result, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // 数据不存在
		}
		return nil, fmt.Errorf("failed to get kline data: %w", err)
	}

	var data KlineData
	if err := json.Unmarshal([]byte(result), &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal kline data: %w", err)
	}

	return &data, nil
}

// 获取最后更新时间
func (c *Client) GetLastUpdateTime(ctx context.Context, symbol, timeframe string) (*time.Time, error) {
	key := c.GetTimestampKey(symbol, timeframe)

	result, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // 时间戳不存在
		}
		return nil, fmt.Errorf("failed to get timestamp: %w", err)
	}

	timestamp, err := time.Parse(TimeFormat, result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	timestamp = timestamp.In(time.FixedZone("CST", 8*3600))

	return &timestamp, nil
}

// 更新系统状态
func (c *Client) UpdateSystemStatus(ctx context.Context, timeframe string, status *SystemStatus) error {
	key := c.GetSystemStatusKey(timeframe)

	jsonData, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal system status: %w", err)
	}

	// 2小时过期
	if err := c.rdb.SetEX(ctx, key, jsonData, 2*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to store system status: %w", err)
	}

	if timeframe != "" {
		if err := c.rdb.SetEX(ctx, c.GetSystemStatusKey(""), jsonData, 2*time.Hour).Err(); err != nil {
			logrus.Warnf("failed to store aggregate system status: %v", err)
		}
	}

	return nil
}

// 获取系统状态
func (c *Client) GetSystemStatus(ctx context.Context, timeframe string) (*SystemStatus, error) {
	key := c.GetSystemStatusKey(timeframe)

	result, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get system status: %w", err)
	}

	var status SystemStatus
	if err := json.Unmarshal([]byte(result), &status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal system status: %w", err)
	}

	return &status, nil
}

// 批量操作支持
func (c *Client) Pipeline() redis.Pipeliner {
    return c.rdb.Pipeline()
}

// ===== Sorted-Set helpers (for orderbook/liquidation/OI data) =====
func (c *Client) ZAdd(ctx context.Context, key string, score float64, member string) error {
    z := &redis.Z{Score: score, Member: member}
    return c.rdb.ZAdd(ctx, key, z).Err()
}

func (c *Client) ZRemRangeByScore(ctx context.Context, key string, min, max float64) error {
    return c.rdb.ZRemRangeByScore(ctx, key, fmt.Sprintf("%f", min), fmt.Sprintf("%f", max)).Err()
}

func (c *Client) ZIncrBy(ctx context.Context, key string, increment float64, member string) error {
    return c.rdb.ZIncrBy(ctx, key, increment, member).Err()
}

type ZWithScore struct {
    Member string
    Score  float64
}

func (c *Client) ZRangeByScoreWithScores(ctx context.Context, key string, min, max float64) ([]ZWithScore, error) {
    res, err := c.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
        Min: fmt.Sprintf("%f", min),
        Max: fmt.Sprintf("%f", max),
    }).Result()
    if err != nil {
        if err == redis.Nil {
            return nil, nil
        }
        return nil, err
    }
    out := make([]ZWithScore, 0, len(res))
    for _, z := range res {
        // go-redis returns Member as interface{}, ensure string
        var m string
        switch v := z.Member.(type) {
        case string:
            m = v
        case []byte:
            m = string(v)
        default:
            b, _ := json.Marshal(v)
            m = string(b)
        }
        if math.IsNaN(z.Score) {
            continue
        }
        out = append(out, ZWithScore{Member: m, Score: z.Score})
    }
    return out, nil
}

// ===== Generic helpers =====
func (c *Client) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
    return c.rdb.SetEX(ctx, key, value, expiration).Err()
}

func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
    return c.rdb.Expire(ctx, key, expiration).Err()
}

// 检查数据新鲜度
func (c *Client) CheckDataFreshness(ctx context.Context, symbols []string, timeframe string, maxAge time.Duration) ([]string, error) {
    var staleSymbols []string
    now := time.Now().In(time.FixedZone("CST", 8*3600))

	for _, symbol := range symbols {
		lastUpdate, err := c.GetLastUpdateTime(ctx, symbol, timeframe)
		if err != nil {
			logrus.Warnf("Error checking freshness for %s: %v", symbol, err)
			staleSymbols = append(staleSymbols, symbol)
			continue
		}

		if lastUpdate == nil {
			staleSymbols = append(staleSymbols, symbol)
			continue
		}

		if now.Sub(*lastUpdate) > maxAge {
			staleSymbols = append(staleSymbols, symbol)
		}
	}

	return staleSymbols, nil
}

// 计算平均数据新鲜度
func (c *Client) CalculateAverageFreshness(ctx context.Context, symbols []string, timeframe string) float64 {
	now := time.Now().In(time.FixedZone("CST", 8*3600))
	var totalAge float64
	var validCount int

	for _, symbol := range symbols {
		lastUpdate, err := c.GetLastUpdateTime(ctx, symbol, timeframe)
		if err != nil || lastUpdate == nil {
			continue
		}

		age := now.Sub(*lastUpdate).Seconds()
		totalAge += age
		validCount++
	}

	if validCount == 0 {
		return 540 // 返回默认最大延迟
	}

	return totalAge / float64(validCount)
}
