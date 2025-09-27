package binance

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strconv"
    "strings"
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

// ===================== 仅 U 本位（fapi）版本 =====================

// NewClient：保持原函数名与签名不变；当 baseURL 为空时，默认使用 fapi 域名
func NewClient(baseURL string, requestsPerSecond int) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = "https://fapi.binance.com"
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		limiter: rate.NewLimiter(rate.Limit(requestsPerSecond), requestsPerSecond),
	}
}

// 发送HTTP请求（GET）
func (c *Client) sendRequest(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	// 速率限制
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limit error: %w", err)
	}

	// 构建URL（全走 fapi 前缀）
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

// 获取交易对信息（U 本位：/fapi/v1/exchangeInfo）
func (c *Client) GetExchangeInfo(ctx context.Context) (*ExchangeInfo, error) {
	params := url.Values{}

	data, err := c.sendRequest(ctx, "/fapi/v1/exchangeInfo", params)
	if err != nil {
		return nil, fmt.Errorf("failed to get exchange info: %w", err)
	}

	var info ExchangeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal exchange info: %w", err)
	}

	return &info, nil
}

// 验证交易对是否有效（基于 U 本位 exchangeInfo）
func (c *Client) ValidateSymbol(ctx context.Context, symbol string) (bool, error) {
	// 转换格式 BTC/USDT -> BTCUSDT
	binanceSymbol := convertSymbolFormat(symbol)

	info, err := c.GetExchangeInfo(ctx)
	if err != nil {
		return false, err
	}

	for _, s := range info.Symbols {
		if strings.EqualFold(s.Symbol, binanceSymbol) && s.Status == "TRADING" {
			return true, nil
		}
	}

	return false, nil
}

// 获取K线数据（U 本位：/fapi/v1/klines）
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

	data, err := c.sendRequest(ctx, "/fapi/v1/klines", params)
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
		// fapi 的 kline 返回至少 12+ 字段，使用到 0,1,2,3,4,5,6
		if len(item) < 7 {
			continue
		}

		openTime, ok0 := toInt64(item[0])
		closeTime, ok6 := toInt64(item[6])
		if !ok0 || !ok6 {
			logrus.Warnf("Failed to parse time fields: open=%v close=%v", item[0], item[6])
			continue
		}

		open, err := toDecimal(item[1])
		if err != nil {
			logrus.Warnf("Failed to parse open price: %v", err)
			continue
		}
		high, err := toDecimal(item[2])
		if err != nil {
			logrus.Warnf("Failed to parse high price: %v", err)
			continue
		}
		low, err := toDecimal(item[3])
		if err != nil {
			logrus.Warnf("Failed to parse low price: %v", err)
			continue
		}
		closep, err := toDecimal(item[4])
		if err != nil {
			logrus.Warnf("Failed to parse close price: %v", err)
			continue
		}
		volume, err := toDecimal(item[5])
		if err != nil {
			logrus.Warnf("Failed to parse volume: %v", err)
			continue
		}

		kline := KlineItem{
			OpenTime:  openTime,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closep,
			Volume:    volume,
			CloseTime: closeTime,
		}

		klines = append(klines, kline)
	}

	return klines, nil
}

// 辅助：把接口返回的数字（string/float64）转 decimal
func toDecimal(v interface{}) (decimal.Decimal, error) {
	switch t := v.(type) {
	case string:
		return decimal.NewFromString(t)
	case float64:
		// 避免精度丢失，转回字符串再解析
		return decimal.NewFromString(strconv.FormatFloat(t, 'f', -1, 64))
	default:
		return decimal.Zero, fmt.Errorf("unexpected number type: %T", v)
	}
}

// 辅助：转 int64（毫秒时间戳等）
func toInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case json.Number:
		i, err := t.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

// 获取历史数据（内部已走 U 本位 klines）
func (c *Client) GetHistoricalData(ctx context.Context, symbol, interval string, days int) ([]KlineItem, error) {
	endTime := time.Now().In(time.FixedZone("CST", 8*3600))
	startTime := endTime.AddDate(0, 0, -days)

	// Binance API每次最多返回1000条数据
	const maxLimit = 1000
	var allKlines []KlineItem

	// 计算需要多少次请求
	intervalDuration := ParseInterval(interval)

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

	logrus.Infof("Fetched %d klines for %s (requested %d days, market=UM-futures)", len(allKlines), symbol, days)
	return allKlines, nil
}

// 工具函数：转换交易对格式 BTC/USDT -> BTCUSDT
func convertSymbolFormat(symbol string) string {
	cleaned := strings.ReplaceAll(symbol, "/", "")
	return strings.ToUpper(cleaned)
}

// 工具函数：解析时间间隔
func ParseInterval(interval string) time.Duration {
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

// 测试连接（U 本位：/fapi/v1/ping）
func (c *Client) TestConnectivity(ctx context.Context) error {
	params := url.Values{}

	_, err := c.sendRequest(ctx, "/fapi/v1/ping", params)
	if err != nil {
		return fmt.Errorf("connectivity test failed: %w", err)
	}

	return nil
}

// ================= Open Interest History (UM futures) =================

type OIItem struct {
    Symbol                 string  `json:"symbol"`
    SumOpenInterest        string  `json:"sumOpenInterest"`
    SumOpenInterestValue   string  `json:"sumOpenInterestValue"`
    Timestamp              int64   `json:"timestamp"`
}

// GetOpenInterestHistory fetches UM futures aggregated open interest history.
// Period supports: 5m, 15m, 30m, 1h, 4h, 1d
func (c *Client) GetOpenInterestHistory(ctx context.Context, symbol, period string, limit int, startTime, endTime *time.Time) ([]OIItem, error) {
    binanceSymbol := convertSymbolFormat(symbol)
    params := url.Values{}
    params.Set("symbol", binanceSymbol)
    params.Set("period", period)
    if limit > 0 { params.Set("limit", strconv.Itoa(limit)) }
    if startTime != nil { params.Set("startTime", strconv.FormatInt(startTime.UnixMilli(), 10)) }
    if endTime != nil { params.Set("endTime", strconv.FormatInt(endTime.UnixMilli(), 10)) }

    data, err := c.sendRequest(ctx, "/futures/data/openInterestHist", params)
    if err != nil {
        return nil, fmt.Errorf("failed to get OI history for %s: %w", symbol, err)
    }
    var items []OIItem
    if err := json.Unmarshal(data, &items); err != nil {
        return nil, fmt.Errorf("failed to unmarshal OI history: %w", err)
    }
    return items, nil
}
