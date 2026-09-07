// ledgerd: a double-entry ledger service.
//
// Composition root: everything is wired here and nowhere else. Lifecycle is
// managed with errgroup + signal.NotifyContext so that SIGTERM drains in-flight
// HTTP requests, stops the outbox relay at a batch boundary, and closes the
// pool - in that order - before the process exits.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sync/errgroup"

	"github.com/dwijamahardika/ledgerd/internal/adapters/httpapi"
	"github.com/dwijamahardika/ledgerd/internal/adapters/postgres"
	"github.com/dwijamahardika/ledgerd/internal/adapters/webhook"
	"github.com/dwijamahardika/ledgerd/internal/app"
	"github.com/dwijamahardika/ledgerd/internal/config"
	"github.com/dwijamahardika/ledgerd/internal/platform/backoff"
	"github.com/dwijamahardika/ledgerd/internal/platform/circuitbreaker"
	"github.com/dwijamahardika/ledgerd/internal/platform/clock"
	"github.com/dwijamahardika/ledgerd/internal/platform/outbox"
	"github.com/dwijamahardika/ledgerd/internal/platform/ratelimit"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- infrastructure ----
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	poolCfg.MaxConns = 20
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	if err := postgres.Migrate(ctx, pool, log); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := httpapi.NewMetrics(reg)

	// ---- application ----
	store := postgres.NewStore(pool)
	svc := app.NewService(store, clock.System{})

	breaker := circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: cfg.BreakerThreshold,
		OpenTimeout:      cfg.BreakerOpenFor,
		OnStateChange: func(from, to circuitbreaker.State) {
			log.Warn("webhook circuit breaker", "from", from.String(), "to", to.String())
			metrics.BreakerState("webhook", int(to))
		},
	}, clock.System{})

	var publisher outbox.Publisher
	if cfg.WebhookURL != "" {
		publisher = webhook.New(cfg.WebhookURL, cfg.WebhookSecret, breaker, cfg.WebhookTimeout)
	} else {
		log.Warn("WEBHOOK_URL not set; events will be logged, not delivered")
		publisher = webhook.LogPublisher{Log: log}
	}
	relay := outbox.NewRelay(postgres.NewOutboxSource(store), publisher, outbox.Config{
		PollInterval: cfg.OutboxPollInterval,
		BatchSize:    cfg.OutboxBatchSize,
		MaxAttempts:  cfg.OutboxMaxAttempts,
		Backoff:      backoff.New(500*time.Millisecond, 5*time.Minute),
		Metrics:      metrics,
	}, log)

	limiter := ratelimit.New(cfg.RateLimitRPS, cfg.RateLimitBurst, clock.System{})

	handler := httpapi.NewHandler(httpapi.Deps{
		Service:     svc,
		Idempotency: postgres.NewIdempotencyStore(store),
		Limiter:     limiter,
		Metrics:     metrics,
		Registry:    reg,
		Logger:      log,
		APIKeys:     cfg.APIKeys,
		Ready:       store.Ping,
		IdemTTL:     cfg.IdempotencyTTL,
		EnablePprof: cfg.EnablePprof,
	})
	server := httpapi.NewServer(cfg.Addr, handler)

	// ---- lifecycle ----
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Info("http listening", "addr", cfg.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down http", "timeout", cfg.ShutdownTimeout)
		shCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		return server.Shutdown(shCtx)
	})
	g.Go(func() error {
		log.Info("outbox relay started", "poll", cfg.OutboxPollInterval, "batch", cfg.OutboxBatchSize)
		if err := relay.Run(gctx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("outbox relay: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		// Janitor for idle rate-limit buckets; a tiny loop instead of a per-key timer.
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				if n := limiter.Sweep(10 * time.Minute); n > 0 {
					log.Debug("rate limiter swept", "evicted", n)
				}
			}
		}
	})

	err = g.Wait()
	log.Info("bye")
	return err
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
