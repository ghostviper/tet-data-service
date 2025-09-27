package volatility

import (
    "context"
    "encoding/json"
    "fmt"
    "sort"
    "strings"
    "time"

    redisClient "tet-data-service/internal/redis"
    "tet-data-service/internal/binance"

    "github.com/sirupsen/logrus"
)

type Service struct {
    redis     *redisClient.Client
    symbols   []string
    timeframe string // e.g. 15m, 1h
    interval  time.Duration
}

func New(redis *redisClient.Client, symbols []string, timeframe string, intervalSec int) *Service {
    if timeframe == "" { timeframe = "15m" }
    if intervalSec <= 0 { intervalSec = 300 }
    return &Service{redis: redis, symbols: symbols, timeframe: timeframe, interval: time.Duration(intervalSec) * time.Second}
}

func (s *Service) Start(ctx context.Context) error {
    go s.loop(ctx)
    logrus.Info("Volatility service started")
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
    tfDur := binance.ParseInterval(s.timeframe)
    if tfDur <= 0 { tfDur = time.Hour }
    candles24h := int((24 * time.Hour) / tfDur)
    if candles24h < 2 { candles24h = 2 }

    for _, sym := range s.symbols {
        // Pull kline data from Redis
        kd, err := s.redis.GetKlineData(ctx, sym, s.timeframe)
        if err != nil {
            logrus.Warnf("Volatility %s: fetch kline error: %v", sym, err)
            continue
        }
        if kd == nil || kd.OHLCVData == "" || kd.CandleCount == 0 {
            logrus.Debugf("Volatility %s: no kline data for %s", sym, s.timeframe)
            continue
        }

        // Parse OHLCV map
        type row struct{ Open, High, Low, Close, Volume float64; TS time.Time }
        var raw map[string]map[string]string
        if err := json.Unmarshal([]byte(kd.OHLCVData), &raw); err != nil {
            logrus.Warnf("Volatility %s: unmarshal OHLCV error: %v", sym, err)
            continue
        }
        rows := make([]row, 0, len(raw))
        for tsStr, vals := range raw {
            ts, err := time.ParseInLocation(redisClient.TimeFormat, tsStr, time.FixedZone("CST", 8*3600))
            if err != nil { continue }
            o := parseFloat(vals["open"]) 
            h := parseFloat(vals["high"]) 
            l := parseFloat(vals["low"]) 
            c := parseFloat(vals["close"]) 
            v := parseFloat(vals["volume"]) 
            rows = append(rows, row{Open: o, High: h, Low: l, Close: c, Volume: v, TS: ts})
        }
        if len(rows) < 10 { continue }
        sort.Slice(rows, func(i, j int) bool { return rows[i].TS.Before(rows[j].TS) })

        // Ensure sufficient history
        if len(rows) < candles24h+1 { 
            logrus.Debugf("Volatility %s: insufficient candles (%d < %d)", sym, len(rows), candles24h+1)
            continue 
        }

        // Compute True Range series
        tr := make([]float64, len(rows))
        prevClose := rows[0].Close
        for i := range rows {
            if i > 0 { prevClose = rows[i-1].Close }
            hl := rows[i].High - rows[i].Low
            hc := abs(rows[i].High - prevClose)
            lc := abs(rows[i].Low - prevClose)
            tr[i] = maxf(hl, maxf(hc, lc))
        }

        // ATR via EMA over 24h window
        atr := ema(tr, candles24h)
        atr24 := atr[len(atr)-1]
        lastClose := rows[len(rows)-1].Close
        atr24Pct := 0.0
        if lastClose > 0 { atr24Pct = (atr24 / lastClose) * 100 }

        // IO 24h change
        io24 := ((lastClose - rows[len(rows)-1-candles24h].Close) / rows[len(rows)-1-candles24h].Close) * 100

        // Volume 24h sum
        vol24 := 0.0
        for i := len(rows)-candles24h; i < len(rows); i++ { vol24 += rows[i].Volume }

        // Score mapping (align with Python thresholds)
        atrLevel, atrScore := scoreATR(atr24Pct)
        ioLevel, ioScore := scoreIO(io24)
        overall := (atrScore + ioScore) / 2.0
        overallLevel := scoreLevel(overall)

        // Build result
        res := map[string]interface{}{
            "current_price": lastClose,
            "volume_24h":    vol24,
            "timeframe_minutes": int(tfDur.Minutes()),
            "atr_24h":       atr24,
            "atr_24h_pct":   atr24Pct,
            "io_24h":        io24,
            "volatility_score": map[string]interface{}{
                "atr_level": atrLevel,
                "atr_score": atrScore,
                "io_level":  ioLevel,
                "io_score":  ioScore,
                "overall_level": overallLevel,
                "overall_score": overall,
            },
            "calculated_at": time.Now().In(time.FixedZone("CST", 8*3600)).Format(time.RFC3339),
            "data_points":   len(rows),
        }

        // Store to Redis with 30m TTL
        key := fmt.Sprintf("tet:volatility:%s", normalize(sym))
        payload, _ := json.Marshal(res)
        if err := s.redis.SetEX(ctx, key, payload, 30*time.Minute); err != nil {
            logrus.Warnf("Volatility %s: store error: %v", sym, err)
            continue
        }
        // Update ranking sorted set (score=overall)
        rankKey := "tet:volatility:ranking"
        _ = s.redis.ZAdd(ctx, rankKey, overall, normalize(sym))
        _ = s.redis.Expire(ctx, rankKey, 30*time.Minute)
        logrus.Debugf("Volatility %s: atr%%=%.2f io%%=%.2f score=%.1f", sym, atr24Pct, io24, overall)
    }
}

// --- helpers ---
func normalize(s string) string { return strings.ReplaceAll(s, "/", "_") }

func parseFloat(s string) float64 { f, _ := strconvParseFloat(s); return f }

func abs(f float64) float64 { if f < 0 { return -f } ; return f }
func maxf(a, b float64) float64 { if a > b { return a } ; return b }

func ema(values []float64, period int) []float64 {
    if period <= 1 { return append([]float64(nil), values...) }
    out := make([]float64, len(values))
    k := 2.0 / (float64(period) + 1)
    out[0] = values[0]
    for i := 1; i < len(values); i++ {
        out[i] = values[i]*k + out[i-1]*(1-k)
    }
    return out
}

func scoreATR(pct float64) (string, float64) {
    switch {
    case pct >= 8:
        return "EXTREME", 100
    case pct >= 5:
        return "HIGH", 80
    case pct >= 3:
        return "MEDIUM", 60
    case pct >= 1.5:
        return "LOW", 40
    default:
        return "MINIMAL", 20
    }
}

func scoreIO(pct float64) (string, float64) {
    v := pct
    if v < 0 { v = -v }
    switch {
    case v >= 15:
        return "EXTREME", 100
    case v >= 10:
        return "HIGH", 80
    case v >= 5:
        return "MEDIUM", 60
    case v >= 2:
        return "LOW", 40
    default:
        return "MINIMAL", 20
    }
}

func scoreLevel(score float64) string {
    switch {
    case score >= 90:
        return "EXTREME"
    case score >= 70:
        return "HIGH"
    case score >= 50:
        return "MEDIUM"
    case score >= 30:
        return "LOW"
    default:
        return "MINIMAL"
    }
}

// minimal wrappers to avoid extra imports
func strconvParseFloat(s string) (float64, error) {
    var f float64
    _, err := fmt.Sscanf(s, "%f", &f)
    return f, err
}

// (no custom string helpers)
