package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	limiter    *rate.Limiter
}

type KlineItem struct {
	OpenTime  int64           `json:"open_time"`
	Open      decimal.Decimal `json:"open"`
	High      decimal.Decimal `json:"high"`
	Low       decimal.Decimal `json:"low"`
	Close     decimal.Decimal `json:"close"`
	Volume    decimal.Decimal `json:"volume"`
	CloseTime int64           `json:"close_time"`
}

type KlineResponse [][]interface{}

type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

type SymbolInfo struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

func NewClient(baseURL string, requestsPerSecond int) *Client {
	return &Client{
		baseURL:   baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		limiter: rate.NewLimiter(rate.Limit(requestsPerSecond), requestsPerSecond),
	}
}

// 发送HTTP请求
func (c *Client) sendRequest(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	// 速率限制
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limit error: %w", err)
	}

	// 构建URL
	fullURL := c.baseURL + endpoint

	if len(params) > 0 {
		fullURL += "?" + params.Encode()
	}

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// 发送请求
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	return body, nil
}

// 获取交易对信息
func (c *Client) GetExchangeInfo(ctx context.Context) (*ExchangeInfo, error) {
	params := url.Values{}

	data, err := c.sendRequest(ctx, "/api/v3/exchangeInfo", params)
	if err != nil {
		return nil, fmt.Errorf("failed to get exchange info: %w", err)
	}

	var info ExchangeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal exchange info: %w", err)
	}

	return &info, nil
}

// 验证交易对是否有效
func (c *Client) ValidateSymbol(ctx context.Context, symbol string) (bool, error) {
	// 转换格式 BTC/USDT -> BTCUSDT
	binanceSymbol := convertSymbolFormat(symbol)

	info, err := c.GetExchangeInfo(ctx)
	if err != nil {
		return false, err
	}

	for _, s := range info.Symbols {
		if s.Symbol == binanceSymbol && s.Status == "TRADING" {
			return true, nil
		}
	}

	return false, nil
}

// 获取K线数据
func (c *Client) GetKlines(ctx context.Context, symbol, interval string, limit int, startTime, endTime *time.Time) ([]KlineItem, error) {
	// 转换交易对格式
	binanceSymbol := convertSymbolFormat(symbol)

	params := url.Values{}
	params.Set("symbol", binanceSymbol)
	params.Set("interval", interval)
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	if startTime != nil {
		params.Set("startTime", strconv.FormatInt(startTime.UnixMilli(), 10))
	}
	if endTime != nil {
		params.Set("endTime", strconv.FormatInt(endTime.UnixMilli(), 10))
	}

	data, err := c.sendRequest(ctx, "/api/v3/klines", params)
	if err != nil {
		return nil, fmt.Errorf("failed to get klines for %s: %w", symbol, err)
	}

	var response KlineResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal klines response: %w", err)
	}

	return c.parseKlineResponse(response)
}

// 解析K线响应数据
func (c *Client) parseKlineResponse(response KlineResponse) ([]KlineItem, error) {
	var klines []KlineItem

	for _, item := range response {
		if len(item) < 6 {
			continue
		}

		// 转换数据类型
		openTime := int64(item[0].(float64))
		closeTime := int64(item[6].(float64))

		open, err := decimal.NewFromString(item[1].(string))
		if err != nil {
			logrus.Warnf("Failed to parse open price: %v", err)
			continue
		}

		high, err := decimal.NewFromString(item[2].(string))
		if err != nil {
			logrus.Warnf("Failed to parse high price: %v", err)
			continue
		}

		low, err := decimal.NewFromString(item[3].(string))
		if err != nil {
			logrus.Warnf("Failed to parse low price: %v", err)
			continue
		}

		close, err := decimal.NewFromString(item[4].(string))
		if err != nil {
			logrus.Warnf("Failed to parse close price: %v", err)
			continue
		}

		volume, err := decimal.NewFromString(item[5].(string))
		if err != nil {
			logrus.Warnf("Failed to parse volume: %v", err)
			continue
		}

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

// 获取历史数据
func (c *Client) GetHistoricalData(ctx context.Context, symbol, interval string, days int) ([]KlineItem, error) {
	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -days)

	// Binance API每次最多返回1000条数据
	const maxLimit = 1000
	var allKlines []KlineItem

	// 计算需要多少次请求
	intervalDuration := parseInterval(interval)
	totalCandles := int(endTime.Sub(startTime) / intervalDuration)

	currentStart := startTime
	for currentStart.Before(endTime) {
		currentEnd := currentStart.Add(time.Duration(maxLimit) * intervalDuration)
		if currentEnd.After(endTime) {
			currentEnd = endTime
		}

		klines, err := c.GetKlines(ctx, symbol, interval, maxLimit, &currentStart, &currentEnd)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch historical data batch: %w", err)
		}

		allKlines = append(allKlines, klines...)
		currentStart = currentEnd

		// 避免过于频繁的请求
		if currentStart.Before(endTime) {
			time.Sleep(100 * time.Millisecond)
		}
	}

	logrus.Infof("Fetched %d klines for %s (requested %d days)", len(allKlines), symbol, days)
	return allKlines, nil
}

// 工具函数：转换交易对格式 BTC/USDT -> BTCUSDT
func convertSymbolFormat(symbol string) string {
	return symbol[0:3] + symbol[4:] // 简单的转换，假设都是 XXX/USDT 格式
}

// 工具函数：解析时间间隔
func parseInterval(interval string) time.Duration {
	switch interval {
	case "1m":
		return time.Minute
	case "3m":
		return 3 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "8h":
		return 8 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "1w":
		return 7 * 24 * time.Hour
	case "1M":
		return 30 * 24 * time.Hour
	default:
		return 15 * time.Minute
	}
}

// 测试连接
func (c *Client) TestConnectivity(ctx context.Context) error {
	params := url.Values{}

	_, err := c.sendRequest(ctx, "/api/v3/ping", params)
	if err != nil {
		return fmt.Errorf("connectivity test failed: %w", err)
	}

	return nil
}
