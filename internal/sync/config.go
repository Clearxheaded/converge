package sync

import "time"

// Config contains timing and size parameters for WebSocket transport and heartbeats.
type Config struct {
	WriteWait      time.Duration
	PongWait       time.Duration
	PingPeriod     time.Duration
	MaxMessageSize int64
}

// DefaultConfig returns production-ready settings.
func DefaultConfig() Config {
	pongWait := 10 * time.Second
	return Config{
		WriteWait:      5 * time.Second,
		PongWait:       pongWait,
		PingPeriod:     (pongWait * 9) / 10,
		MaxMessageSize: 1024 * 1024, // 1MB
	}
}

// FastConfig returns tuned parameters for fast, deterministic unit and integration tests.
func FastConfig() Config {
	pongWait := 1500 * time.Millisecond
	return Config{
		WriteWait:      1 * time.Second,
		PongWait:       pongWait,
		PingPeriod:     500 * time.Millisecond,
		MaxMessageSize: 512 * 1024,
	}
}
