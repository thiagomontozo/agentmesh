package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/cache"
	"github.com/thiagomontozo/agentmesh/internal/config"
	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/httpapi"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
	"github.com/thiagomontozo/agentmesh/internal/store"
	postgresstore "github.com/thiagomontozo/agentmesh/internal/store/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var repository store.Repository
	var runQueue queue.Queue
	var coordinator coordination.Coordinator
	var eventBus events.Broker
	if cfg.Mode == "distributed" {
		postgresRepository, err := postgresstore.New(rootCtx, cfg.DatabaseURL)
		if err != nil {
			slog.Error("PostgreSQL initialization failed", "error", err)
			os.Exit(1)
		}
		defer postgresRepository.Close()

		redisCache, err := cache.NewRedis(cfg.RedisURL)
		if err != nil {
			slog.Error("Redis initialization failed", "error", err)
			os.Exit(1)
		}
		defer func() { _ = redisCache.Close() }()
		if err := redisCache.Ping(rootCtx); err != nil {
			slog.Error("Redis is unavailable", "error", err)
			os.Exit(1)
		}

		natsQueue, err := queue.NewNATS(rootCtx, cfg.NATSURL, cfg.NATSAckWait)
		if err != nil {
			slog.Error("NATS initialization failed", "error", err)
			os.Exit(1)
		}
		repository = store.NewCached(postgresRepository, redisCache, cfg.CacheTTL)
		runQueue = natsQueue
		coordinator = redisCache

		natsEvents, err := events.NewPersistentNATS(
			cfg.NATSURL, repository, cfg.EventRetention, cfg.EventHistoryLimit,
		)
		if err != nil {
			slog.Error("distributed event bus initialization failed", "error", err)
			os.Exit(1)
		}
		defer func() { _ = natsEvents.Close() }()
		eventBus = natsEvents
	} else {
		repository = store.NewMemory()
		runQueue = queue.NewMemory(cfg.QueueSize)
		coordinator = coordination.NewMemory()
		eventBus = events.NewBus()
	}

	executor := engine.DemoExecutor{Delay: cfg.ExecutionDelay}
	runtimeResolver := agentruntime.NewRegistry(agentruntime.AdaptLegacy(executor))
	if err := runtimeResolver.Register(
		agentruntime.RemoteRuntime,
		agentruntime.NewHTTPRuntime(&http.Client{Timeout: cfg.AttemptTimeout}, 0),
	); err != nil {
		slog.Error("HTTP runtime registration failed", "error", err)
		os.Exit(1)
	}
	runEngine := engine.NewWithResolver(repository, eventBus, runtimeResolver, runQueue, coordinator, cfg.Workers, engine.RetryPolicy{
		MaxAttempts:    cfg.MaxAttempts,
		InitialBackoff: cfg.RetryInitial,
		MaxBackoff:     cfg.RetryMax,
		LeaseTTL:       cfg.LeaseTTL,
		AttemptTimeout: cfg.AttemptTimeout,
	})
	if err := runEngine.Recover(rootCtx); err != nil {
		slog.Error("run recovery failed", "error", err)
		os.Exit(1)
	}
	runEngine.Start(rootCtx)

	api := httpapi.New(repository, runEngine, eventBus)
	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("agentmesh started", "addr", cfg.Addr, "workers", cfg.Workers)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-rootCtx.Done():
		slog.Info("shutdown signal received")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "error", err)
		}
		stop()
	case err := <-runEngine.Errors():
		slog.Error("run engine failed", "error", err)
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown failed", "error", err)
	}
	runEngine.Stop()
	slog.Info("agentmesh stopped")
}
