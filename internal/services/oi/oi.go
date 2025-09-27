package oi

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/sirupsen/logrus"
    "tet-data-service/internal/binance"
    redisClient "tet-data-service/internal/redis"
    "strings"
)

type Service struct {
    binance   *binance.Client
    redis     *redisClient.Client
    symbols   []string
    interval  time.Duration
    period    string // 5m/15m/1h etc.
}

func New(bc *binance.Client, rc *redisClient.Client, symbols []string, period string, intervalSec int) *Service {
    if intervalSec <= 0 { intervalSec = 300 }
    return &Service{binance: bc, redis: rc, symbols: symbols, period: period, interval: time.Duration(intervalSec) * time.Second}
}

func (s *Service) Start(ctx context.Context) error {
    go s.loop(ctx)
    return nil
}

func (s *Service) loop(ctx context.Context) {
    ticker := time.NewTicker(s.interval)
    defer ticker.Stop()
    for {
        s.runOnce(ctx)
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
        }
    }
}

func (s *Service) runOnce(ctx context.Context) {
    end := time.Now()
    start := end.Add(-2 * time.Hour) // fetch 2h history
    for _, sym := range s.symbols {
        items, err := s.binance.GetOpenInterestHistory(ctx, sym, s.period, 1000, &start, &end)
        if err != nil {
            logrus.Warnf("OI %s error: %v", sym, err)
            continue
        }
        // Keep slash format in key for OI, matching existing consumers
        key := fmt.Sprintf("open_interest:%s", sym)
        for _, it := range items {
            payloadObj := map[string]interface{}{
                "symbol":                formatOISymbol(it.Symbol, sym),
                "sumOpenInterest":       it.SumOpenInterest,
                "sumOpenInterestValue":  it.SumOpenInterestValue,
                "timestamp":             it.Timestamp,
            }
            payload, _ := json.Marshal(payloadObj)
            _ = s.redis.ZAdd(ctx, key, float64(it.Timestamp), string(payload))
        }
        cutoff := float64(time.Now().Add(-24 * time.Hour).UnixMilli())
        _ = s.redis.ZRemRangeByScore(ctx, key, 0, cutoff)
        logrus.Infof("OI %s: %d records", sym, len(items))
    }
}

// normalize converts symbol like "BTC/USDT" to "BTC_USDT" for key consistency
func normalize(s string) string { return strings.ReplaceAll(s, "/", "_") }

// formatSlashSymbol ensures the JSON field 'symbol' is a pair like "BTC/USDT"
// If Binance returns compact symbol (e.g., BTCUSDT), prefer the original input 'sym' if it already contains '/'
func formatSlashSymbol(binanceSymbol string, original string) string {
    if strings.Contains(original, "/") {
        return original
    }
    // Fallback: derive from Binance compact symbol for common quotes
    upper := strings.ToUpper(binanceSymbol)
    quotes := []string{"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB"}
    for _, q := range quotes {
        if strings.HasSuffix(upper, q) {
            base := strings.TrimSuffix(upper, q)
            return base + "/" + q
        }
    }
    return original
}

// formatOISymbol ensures symbol field is like "BASE/QUOTE:QUOTE" (e.g., "ADA/USDT:USDT")
func formatOISymbol(binanceSymbol string, original string) string {
    slash := formatSlashSymbol(binanceSymbol, original)
    // Extract quote from slash format
    parts := strings.Split(slash, "/")
    quote := "USDT"
    if len(parts) == 2 && parts[1] != "" {
        quote = parts[1]
    } else {
        // Fallback from compact Binance symbol
        upper := strings.ToUpper(binanceSymbol)
        quotes := []string{"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB"}
        for _, q := range quotes {
            if strings.HasSuffix(upper, q) {
                quote = q
                break
            }
        }
    }
    return slash + ":" + quote
}
