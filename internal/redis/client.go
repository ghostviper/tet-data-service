package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type Client struct {
	rdb *redis.Client
}

type KlineData struct {
	OHLCVData      string    `json:"ohlcv_data"`
	LastCandleTime string    `json:"last_candle_time"`
	LastUpdate     string    `json:"last_update"`
	DataStart      string    `json:"data_start"`
	DataEnd        string    `json:"data_end"`
	Completeness   float64   `json:"data_completeness"`
	CandleCount    int       `json:"candle_count"`
	Timeframe      string    `json:"timeframe"`
	DataAgeSeconds float64   `json:"data_age_seconds"`
}

type SystemStatus struct {
	LastFullScan        string  `json:"last_full_scan"`
	ActiveSymbols       int     `json:"active_symbols"`
	SuccessfulUpdates   int     `json:"successful_updates"`
	FailedUpdates       int     `json:"failed_updates"`
	UpdateDuration      float64 `json:"update_duration_seconds"`
	DataFreshnessAvg    float64 `json:"data_freshness_avg"`
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
	return fmt.Sprintf("tet:kline:%s:%s", timeframe, symbol)
}

func (c *Client) GetTimestampKey(symbol, timeframe string) string {
	return fmt.Sprintf("tet:timestamp:%s:%s", timeframe, symbol)
}

func (c *Client) GetSystemStatusKey() string {
	return "tet:system:status"
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
	now := time.Now().Format(time.RFC3339)
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

	timestamp, err := time.Parse(time.RFC3339, result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return &timestamp, nil
}

// 更新系统状态
func (c *Client) UpdateSystemStatus(ctx context.Context, status *SystemStatus) error {
	key := c.GetSystemStatusKey()

	jsonData, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal system status: %w", err)
	}

	// 2小时过期
	if err := c.rdb.SetEX(ctx, key, jsonData, 2*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to store system status: %w", err)
	}

	return nil
}

// 获取系统状态
func (c *Client) GetSystemStatus(ctx context.Context) (*SystemStatus, error) {
	key := c.GetSystemStatusKey()

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

// 检查数据新鲜度
func (c *Client) CheckDataFreshness(ctx context.Context, symbols []string, timeframe string, maxAge time.Duration) ([]string, error) {
	var staleSymbols []string
	now := time.Now()

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
	now := time.Now()
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