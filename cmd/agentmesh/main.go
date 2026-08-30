package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/agenthealth"
	"github.com/thiagomontozo/agentmesh/internal/apiauth"
	"github.com/thiagomontozo/agentmesh/internal/cache"
	"github.com/thiagomontozo/agentmesh/internal/config"
	"github.com/thiagomontozo/agentmesh/internal/coordination"
	"github.com/thiagomontozo/agentmesh/internal/engine"
	"github.com/thiagomontozo/agentmesh/internal/events"
	"github.com/thiagomontozo/agentmesh/internal/httpapi"
	metricspkg "github.com/thiagomontozo/agentmesh/internal/metrics"
	"github.com/thiagomontozo/agentmesh/internal/queue"
	agentruntime "github.com/thiagomontozo/agentmesh/internal/runtime"
	"github.com/thiagomontozo/agentmesh/internal/store"
	postgresstore "github.com/thiagomontozo/agentmesh/internal/store/postgres"
	workflowengine "github.com/thiagomontozo/agentmesh/internal/workflow"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	instanceID := resolveInstanceID(cfg.InstanceID)
	logger := slog.Default().With("instance_id", instanceID)

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var repository store.Repository
	var runQueue queue.Queue
	var coordinator coordination.Coordinator
	var eventBus events.Broker
	if cfg.Mode == "distributed" {
		postgresRepository, err := postgresstore.New(rootCtx, cfg.DatabaseURL)
		if err != nil {
			logger.Error("PostgreSQL initialization failed", "error", err)
			os.Exit(1)
		}
		defer postgresRepository.Close()

		redisCache, err := cache.NewRedis(cfg.RedisURL)
		if err != nil {
			logger.Error("Redis initialization failed", "error", err)
			os.Exit(1)
		}
		defer func() { _ = redisCache.Close() }()
		if err := redisCache.Ping(rootCtx); err != nil {
			logger.Error("Redis is unavailable", "error", err)
			os.Exit(1)
		}

		natsQueue, err := queue.NewNATS(rootCtx, cfg.NATSURL, cfg.NATSAckWait)
		if err != nil {
			logger.Error("NATS initialization failed", "error", err)
			os.Exit(1)
		}
		repository = store.NewCached(postgresRepository, redisCache, cfg.CacheTTL)
		runQueue = natsQueue
		coordinator = redisCache

		natsEvents, err := events.NewPersistentNATS(
			cfg.NATSURL, repository, cfg.EventRetention, cfg.EventHistoryLimit,
		)
		if err != nil {
			logger.Error("distributed event bus initialization failed", "error", err)
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
	metricsRegistry := metricspkg.New()
	eventBus = metricspkg.WrapBroker(eventBus, metricsRegistry)
	apiAuthenticator, err := apiauth.New(cfg.APIAuthConfig, os.LookupEnv)
	if err != nil {
		logger.Error("API authentication configuration failed", "error", err)
		os.Exit(1)
	}

	executor := engine.DemoExecutor{Delay: cfg.ExecutionDelay}
	runtimeResolver := agentruntime.NewRegistry(agentruntime.AdaptLegacy(executor))
	authenticator, err := agentruntime.NewReloadingAuthenticator(
		cfg.AgentAuthConfig, agentruntime.NewEnvironmentFileSecretProvider(os.LookupEnv),
	)
	if err != nil {
		logger.Error("Agent authentication configuration failed", "error", err)
		os.Exit(1)
	}
	httpRuntime, err := agentruntime.NewSecureHTTPRuntime(&http.Client{Timeout: cfg.AttemptTimeout}, agentruntime.HTTPOptions{
		MaxRequestBytes: cfg.HTTPMaxRequestBytes, MaxResponseBytes: cfg.HTTPMaxResponseBytes,
		Policy: agentruntime.HTTPPolicy{
			RequireHTTPS: cfg.HTTPRequireHTTPS, AllowPrivate: cfg.HTTPAllowPrivate,
			AllowLoopback: cfg.HTTPAllowLoopback, AllowLinkLocal: cfg.HTTPAllowLinkLocal,
			AllowedHosts: cfg.HTTPAllowedHosts, BlockedCIDRs: cfg.HTTPBlockedCIDRs,
		},
		Authenticator: authenticator,
	})
	if err != nil {
		logger.Error("HTTP runtime security configuration failed", "error", err)
		os.Exit(1)
	}
	if err := runtimeResolver.Register(
		agentruntime.RemoteRuntime,
		httpRuntime,
	); err != nil {
		logger.Error("HTTP runtime registration failed", "error", err)
		os.Exit(1)
	}
	runEngine := engine.NewWithResolver(repository, eventBus, runtimeResolver, runQueue, coordinator, cfg.Workers, engine.RetryPolicy{
		MaxAttempts:    cfg.MaxAttempts,
		InitialBackoff: cfg.RetryInitial,
		MaxBackoff:     cfg.RetryMax,
		LeaseTTL:       cfg.LeaseTTL,
		AttemptTimeout: cfg.AttemptTimeout,
	})
	runEngine.SetInstanceID(instanceID)
	runsWork := cfg.Role == "all" || cfg.Role == "worker"
	servesAPI := cfg.Role == "all" || cfg.Role == "api"
	if runsWork {
		if err := runEngine.Recover(rootCtx); err != nil {
			logger.Error("run recovery failed", "error", err)
			os.Exit(1)
		}
		runEngine.Start(rootCtx)
	}
	workflowManager := workflowengine.NewWithCoordinator(repository, runEngine, coordinator, cfg.WorkflowConcurrency, cfg.WorkflowLeaseTTL)
	if servesAPI {
		workflowManager.Run(rootCtx)
		if err := workflowManager.Recover(rootCtx); err != nil {
			logger.Error("workflow recovery failed", "error", err)
			os.Exit(1)
		}
	}
	var healthService *agenthealth.Service
	var server *http.Server
	var metricsServer *http.Server
	var serverErrors <-chan error
	if servesAPI {
		healthService, err = agenthealth.New(repository, nil, agenthealth.Config{
			Path: cfg.AgentHealthPath, Interval: cfg.AgentHealthInterval,
			Timeout: cfg.AgentHealthTimeout, Workers: cfg.AgentHealthWorkers,
		})
		if err != nil {
			logger.Error("agent health initialization failed", "error", err)
			os.Exit(1)
		}
		healthService.Start(rootCtx)

		api := httpapi.NewWithInstanceID(repository, runEngine, eventBus, instanceID)
		api.SetAPISecurity(apiAuthenticator, cfg.AuditRetention, cfg.AuditMaxEvents)
		api.SetMetrics(metricsRegistry)
		api.SetAgentHealth(healthService)
		api.SetWorkflowController(workflowManager)
		api.SetAgentCallLimits(cfg.AgentCallMaxDepth, cfg.AgentCallMaxChildren)
		server = &http.Server{
			Addr:              cfg.Addr,
			Handler:           api.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}

		errorsChannel := make(chan error, 1)
		serverErrors = errorsChannel
		go func() {
			logger.Info("agentmesh API started", "addr", cfg.Addr, "role", cfg.Role, "workers", cfg.Workers)
			errorsChannel <- server.ListenAndServe()
		}()
	} else {
		logger.Info("agentmesh worker started", "role", cfg.Role, "workers", cfg.Workers)
		if cfg.MetricsAddr != "" {
			metricsServer = &http.Server{
				Addr: cfg.MetricsAddr, Handler: apiAuthenticator.Middleware(metricsRegistry.Handler(repository)),
				ReadHeaderTimeout: 5 * time.Second,
			}
			errorsChannel := make(chan error, 1)
			serverErrors = errorsChannel
			go func() {
				logger.Info("agentmesh worker metrics started", "addr", cfg.MetricsAddr)
				errorsChannel <- metricsServer.ListenAndServe()
			}()
		}
	}
	var engineErrors <-chan error
	if runsWork {
		engineErrors = runEngine.Errors()
	}

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
		}
		stop()
	case err := <-engineErrors:
		logger.Error("run engine failed", "error", err)
		stop()
	}

	if server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "error", err)
		}
		cancel()
	}
	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics shutdown failed", "error", err)
		}
		cancel()
	}
	if healthService != nil {
		healthService.Stop()
	}
	workflowManager.Stop()
	runEngine.Stop()
	logger.Info("agentmesh stopped", "role", cfg.Role)
}

func resolveInstanceID(configured string) string {
	if configured != "" {
		return configured
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "agentmesh"
	}
	random := make([]byte, 4)
	if _, err := rand.Read(random); err == nil {
		return fmt.Sprintf("%s-%s", hostname, hex.EncodeToString(random))
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
