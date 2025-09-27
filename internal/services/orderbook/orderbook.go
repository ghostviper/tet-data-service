package orderbook

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "math"
    "net/http"
    "net/url"
    "strings"
    "time"

    "github.com/gorilla/websocket"
    "github.com/sirupsen/logrus"
    redisClient "tet-data-service/internal/redis"
)

type Service struct {
    redis        *redisClient.Client
    symbols      []string
    retention    time.Duration
}

// Depth ranges aligned with Python implementation
var depthRanges = [][2]float64{
    {0.005, 0.015}, // 0.5-1.5%
    {0.01, 0.025},  // 1-2.5%
    {0.02, 0.05},   // 2-5%
}

type orderbookState struct {
    bids          map[float64]float64
    asks          map[float64]float64
    lastUpdateID  int64
    currentPrice  float64
}

// New creates the orderbook service. Symbols should be in `BTC/USDT` format.
func New(redis *redisClient.Client, symbols []string, retentionHours int) *Service {
    return &Service{
        redis:     redis,
        symbols:   symbols,
        retention: time.Duration(retentionHours) * time.Hour,
    }
}

func (s *Service) Start(ctx context.Context) error {
    logrus.Info("Orderbook service starting…")
    for _, sym := range s.symbols {
        go s.runSymbol(ctx, sym)
    }
    return nil
}

func (s *Service) runSymbol(ctx context.Context, symbol string) {
    binanceSym := strings.ToLower(strings.ReplaceAll(symbol, "/", ""))
    wsURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@depth@1000ms", binanceSym)

    backoff := time.Second
    maxBackoff := 30 * time.Second
    var state *orderbookState

    for ctx.Err() == nil {
        conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
        if err != nil {
            logrus.Errorf("%s: orderbook dial error: %v", symbol, err)
            time.Sleep(backoff)
            backoff = minDuration(backoff*2, maxBackoff)
            continue
        }
        logrus.Infof("%s: orderbook websocket connected", symbol)
        backoff = time.Second

        // Reset state by fetching snapshot via REST spot API
        snap, err := fetchDepthSnapshot(binanceSym)
        if err != nil {
            logrus.Warnf("%s: fetch snapshot failed: %v", symbol, err)
            // still proceed; state may be nil and we only update on valid events
        } else {
            state = &orderbookState{
                bids:         snap.Bids,
                asks:         snap.Asks,
                lastUpdateID: snap.LastUpdateID,
                currentPrice: snap.CurrentAsk,
            }
        }

        lastMinute := int64(0)

        for ctx.Err() == nil {
            _, message, err := conn.ReadMessage()
            if err != nil {
                logrus.Errorf("%s: read error: %v", symbol, err)
                _ = conn.Close()
                break
            }
            var evt depthEvent
            if err := json.Unmarshal(message, &evt); err != nil {
                continue
            }

            if state == nil {
                // No state yet; try to get snapshot once
                snap, err := fetchDepthSnapshot(binanceSym)
                if err != nil {
                    continue
                }
                state = &orderbookState{bids: snap.Bids, asks: snap.Asks, lastUpdateID: snap.LastUpdateID, currentPrice: snap.CurrentAsk}
            }

            // Apply event
            if evt.U <= state.lastUpdateID {
                continue
            }
            if evt.UStart <= state.lastUpdateID+1 && evt.U >= state.lastUpdateID+1 {
                for _, b := range evt.B {
                    price, qty := parsePair(b)
                    if qty == 0 {
                        delete(state.bids, price)
                    } else {
                        state.bids[price] = qty
                    }
                }
                for _, a := range evt.A {
                    price, qty := parsePair(a)
                    if qty == 0 {
                        delete(state.asks, price)
                    } else {
                        state.asks[price] = qty
                    }
                }
                state.lastUpdateID = evt.U
                if len(state.asks) > 0 {
                    state.currentPrice = minKey(state.asks)
                }
            }

            // Per-minute signal calculation
            minute := time.Now().Unix() / 60
            if minute > lastMinute && state != nil && state.currentPrice > 0 && len(state.bids) > 0 && len(state.asks) > 0 {
                s.calculateAndStore(symbol, state)
                lastMinute = minute
            }
        }
    }
}

func (s *Service) calculateAndStore(symbol string, st *orderbookState) {
    nowMinute := (time.Now().Unix() / 60) * 60
    cutoff := time.Now().Add(-s.retention).Unix()
    for i, rng := range depthRanges {
        pressure, totalAsks, totalBids := calcPressure(st.bids, st.asks, st.currentPrice, rng[0], rng[1])
        sig := map[string]interface{}{
            "buy_volume":  totalBids,
            "sell_volume": totalAsks,
            "bid_depth":   totalBids,
            "ask_depth":   totalAsks,
            "pressure":    pressure,
            "timestamp":   nowMinute,
            "current_price": st.currentPrice,
        }
        payload, _ := json.Marshal(sig)
        key := fmt.Sprintf("orderbook_signals:%s:depth_%d", normalize(symbol), i)
        // score = minute timestamp
        _ = s.redis.ZAdd(context.Background(), key, float64(nowMinute), string(payload))
        _ = s.redis.ZRemRangeByScore(context.Background(), key, 0, float64(cutoff))
    }
}

// --- helpers ---
func normalize(s string) string { return strings.ReplaceAll(s, "/", "") }

func minDuration(a, b time.Duration) time.Duration {
    if a < b { return a }
    return b
}

func parsePair(p []interface{}) (price float64, qty float64) {
    // p[0] price, p[1] qty as strings
    if len(p) < 2 { return 0, 0 }
    price = toFloat(p[0])
    qty = toFloat(p[1])
    return
}

func toFloat(v interface{}) float64 {
    switch t := v.(type) {
    case string:
        f, _ := strconvParseFloat(t)
        return f
    case float64:
        return t
    default:
        return 0
    }
}

func strconvParseFloat(s string) (float64, error) {
    // minimal wrapper to avoid importing strconv
    var f float64
    _, err := fmt.Sscanf(s, "%f", &f)
    return f, err
}

func minKey(m map[float64]float64) float64 {
    mk := math.MaxFloat64
    for k := range m { if k < mk { mk = k } }
    if mk == math.MaxFloat64 { return 0 }
    return mk
}

func calcPressure(bids, asks map[float64]float64, current float64, lowPct, highPct float64) (pressure float64, totalAsks, totalBids float64) {
    if current <= 0 || len(bids) == 0 || len(asks) == 0 {
        return 0, 0, 0
    }
    // ranges based on best bid/ask
    maxBid := 0.0
    for p := range bids { if p > maxBid { maxBid = p } }
    minAsk := minKey(asks)

    bidLow := maxBid * (1 - highPct)
    bidHigh := maxBid * (1 - lowPct)
    askLow := minAsk * (1 + lowPct)
    askHigh := minAsk * (1 + highPct)

    for price, amount := range bids {
        if price >= bidLow && price <= bidHigh {
            totalBids += amount
        }
    }
    for price, amount := range asks {
        if price >= askLow && price <= askHigh {
            totalAsks += amount
        }
    }
    denom := totalBids + totalAsks
    if denom > 0 {
        pressure = (totalBids - totalAsks) / denom
    }
    return
}

// --- Binance spot depth snapshot (simple REST) ---
type depthSnapshot struct {
    Bids         map[float64]float64
    Asks         map[float64]float64
    LastUpdateID int64
    CurrentAsk   float64
}

type depthEvent struct {
    UStart int64           `json:"U"`
    U      int64           `json:"u"`
    B      [][]interface{} `json:"b"`
    A      [][]interface{} `json:"a"`
}

func fetchDepthSnapshot(symbolLower string) (*depthSnapshot, error) {
    // Using spot REST: https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000
    endpoint := "https://api.binance.com/api/v3/depth"
    params := url.Values{"symbol": []string{strings.ToUpper(symbolLower)}, "limit": []string{"1000"}}
    full := endpoint + "?" + params.Encode()
    client := &http.Client{Timeout: 15 * time.Second}
    req, _ := http.NewRequest("GET", full, nil)
    resp, err := client.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil { return nil, err }
    if resp.StatusCode != 200 { return nil, fmt.Errorf("depth snapshot error: %s", string(body)) }
    var raw struct {
        LastUpdateID int64           `json:"lastUpdateId"`
        Bids         [][]interface{} `json:"bids"`
        Asks         [][]interface{} `json:"asks"`
    }
    if err := json.Unmarshal(body, &raw); err != nil { return nil, err }
    bids := make(map[float64]float64, len(raw.Bids))
    asks := make(map[float64]float64, len(raw.Asks))
    for _, b := range raw.Bids { price, qty := parsePair(b); if price > 0 { bids[price] = qty } }
    for _, a := range raw.Asks { price, qty := parsePair(a); if price > 0 { asks[price] = qty } }
    snap := &depthSnapshot{Bids: bids, Asks: asks, LastUpdateID: raw.LastUpdateID, CurrentAsk: minKey(asks)}
    return snap, nil
}
