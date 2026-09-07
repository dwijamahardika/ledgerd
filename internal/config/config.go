// Package config reads 12-factor style environment variables and validates
// them up front so a misconfigured deploy fails at boot, not on first request.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr            string
	DatabaseURL     string
	APIKeys         map[string]string // key -> principal
	LogLevel        string
	ShutdownTimeout time.Duration
	EnablePprof     bool

	RateLimitRPS   float64
	RateLimitBurst int

	IdempotencyTTL time.Duration

	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxMaxAttempts  int

	WebhookURL       string
	WebhookSecret    string
	WebhookTimeout   time.Duration
	BreakerThreshold int
	BreakerOpenFor   time.Duration
}

func Load() (*Config, error) {
	c := &Config{
		Addr:               env("ADDR", ":8080"),
		DatabaseURL:        env("DATABASE_URL", ""),
		LogLevel:           env("LOG_LEVEL", "info"),
		ShutdownTimeout:    envDur("SHUTDOWN_TIMEOUT", 20*time.Second),
		EnablePprof:        envBool("ENABLE_PPROF", false),
		RateLimitRPS:       envFloat("RATE_LIMIT_RPS", 20),
		RateLimitBurst:     envInt("RATE_LIMIT_BURST", 40),
		IdempotencyTTL:     envDur("IDEMPOTENCY_TTL", 24*time.Hour),
		OutboxPollInterval: envDur("OUTBOX_POLL_INTERVAL", time.Second),
		OutboxBatchSize:    envInt("OUTBOX_BATCH_SIZE", 100),
		OutboxMaxAttempts:  envInt("OUTBOX_MAX_ATTEMPTS", 10),
		WebhookURL:         env("WEBHOOK_URL", ""),
		WebhookSecret:      env("WEBHOOK_SECRET", ""),
		WebhookTimeout:     envDur("WEBHOOK_TIMEOUT", 5*time.Second),
		BreakerThreshold:   envInt("BREAKER_FAILURE_THRESHOLD", 5),
		BreakerOpenFor:     envDur("BREAKER_OPEN_TIMEOUT", 30*time.Second),
		APIKeys:            map[string]string{},
	}

	// API_KEYS="key1:alice,key2:bob" or bare "key1,key2" (principal = key index).
	for i, pair := range strings.Split(env("API_KEYS", ""), ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, name, found := strings.Cut(pair, ":")
		if !found {
			name = fmt.Sprintf("client-%d", i+1)
		}
		c.APIKeys[key] = name
	}

	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if len(c.APIKeys) == 0 {
		errs = append(errs, errors.New("API_KEYS is required (comma-separated, optionally key:name)"))
	}
	if c.RateLimitRPS <= 0 || c.RateLimitBurst <= 0 {
		errs = append(errs, errors.New("RATE_LIMIT_RPS and RATE_LIMIT_BURST must be > 0"))
	}
	if c.WebhookURL != "" && c.WebhookSecret == "" {
		errs = append(errs, errors.New("WEBHOOK_SECRET is required when WEBHOOK_URL is set"))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return c, nil
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(env(k, "")); err == nil {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v, err := strconv.ParseFloat(env(k, ""), 64); err == nil {
		return v
	}
	return def
}

func envBool(k string, def bool) bool {
	if v, err := strconv.ParseBool(env(k, "")); err == nil {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(k, "")); err == nil {
		return v
	}
	return def
}
