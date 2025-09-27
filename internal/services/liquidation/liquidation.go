package liquidation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	redisClient "tet-data-service/internal/redis"
)

type Service struct {
	redis     *redisClient.Client
	retention time.Duration
}

func New(redis *redisClient.Client, retentionHours int) *Service {
	return &Service{redis: redis, retention: time.Duration(retentionHours) * time.Hour}
}

func (s *Service) Start(ctx context.Context) error {
	go s.run(ctx)
	return nil
}

func (s *Service) run(ctx context.Context) {
	url := "wss://fstream.binance.com/ws/!forceOrder@arr"
	backoff := time.Second
	for ctx.Err() == nil {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			logrus.Errorf("liquidation dial error: %v", err)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		logrus.Info("liquidation websocket connected")
		backoff = time.Second
		for ctx.Err() == nil {
			_, message, err := conn.ReadMessage()
			if err != nil {
				_ = conn.Close()
				break
			}
			var evt map[string]any
			if err := json.Unmarshal(message, &evt); err != nil {
				continue
			}
			s.handleEvent(ctx, evt)
		}
	}
}

func (s *Service) handleEvent(ctx context.Context, evt map[string]any) {
	// The stream can send single or array; handle both
	if arr, ok := evt["o"]; ok {
		s.processOne(ctx, map[string]any{"o": arr, "E": evt["E"]})
		return
	}
	// try array form
	if list, ok := evt["data"].([]any); ok {
		for _, it := range list {
			if m, ok := it.(map[string]any); ok {
				s.processOne(ctx, m)
			}
		}
	}
}

func (s *Service) processOne(ctx context.Context, data map[string]any) {
	o, ok := data["o"].(map[string]any)
	if !ok {
		return
	}
	symbol, _ := o["s"].(string)
	side, _ := o["S"].(string)
	q := toFloat(o["q"])     // quantity
	ap := toFloat(o["ap"])   // price
	ts := toInt64(data["E"]) // event time ms
	value := q * ap

	// raw record
	rawKey := fmt.Sprintf("liquidation:%s", symbol)
	payload, _ := json.Marshal(data)
	_ = s.redis.ZAdd(ctx, rawKey, float64(ts), string(payload))
	cutoff := float64(time.Now().Add(-s.retention).UnixMilli())
	_ = s.redis.ZRemRangeByScore(ctx, rawKey, 0, cutoff)

	minute := (ts / 60000) * 60000
	if side == "SELL" { // long liquidation
		_ = s.redis.ZIncrBy(ctx, fmt.Sprintf("liquidation_long:%s", symbol), value, fmt.Sprintf("%d", minute))
	} else {
		_ = s.redis.ZIncrBy(ctx, fmt.Sprintf("liquidation_short:%s", symbol), value, fmt.Sprintf("%d", minute))
	}
	// Aggregated ZSET uses the minute timestamp as the member and liquidation value as the score.
	// Score-based cleanup would remove recent buckets; retention should target members externally.
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case string:
		var f float64
		fmt.Sscanf(t, "%f", &f)
		return f
	case float64:
		return t
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	default:
		return time.Now().UnixMilli()
	}
}
