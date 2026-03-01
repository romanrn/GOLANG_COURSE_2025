package main

import (
	"context"
	"football-tracker/cmd/server"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers"
	"football-tracker/cmd/server/logger"
	"football-tracker/cmd/server/middlewares"
	"football-tracker/internal/clients"
	"football-tracker/internal/out/database"
	"football-tracker/internal/services"
	"football-tracker/internal/telemetry"
	"log/slog"
)

// @title           Football Tracker API
// @version         1.0
// @description     REST API for tracking football championships, matches, and user predictions. Supports multi-championship tracking with user prediction system, rating, and leaderboard functionality.

// @contact.name   Football Tracker API Support
// @contact.email  support@football-tracker.com

// @license.name  Proprietary
// @license.url   #

// @host      localhost
// @BasePath  /
// @schemes   https http

// @securityDefinitions.apikey CookieAuth
// @in cookie
// @name session_token
// @description Session token obtained from /api/v1/auth/login endpoint. After successful login, this cookie is automatically set.

func main() {
	ctx := context.Background()

	cfg, err := config.NewConfigFromEnv()
	if err != nil {
		slog.ErrorContext(ctx, err.Error())
		panic(err)
	}

	logger.InitLogger(cfg)

	// Initialize OpenTelemetry tracing
	shutdownTracer, err := telemetry.InitTracer(ctx, telemetry.Config{
		ServiceName: cfg.OtelServiceName,
		Endpoint:    cfg.OtelEndpoint,
		Enabled:     cfg.OtelEnabled,
	})
	if err != nil {
		logger.GetLogger().Error(ctx, "Failed to initialize tracer", slog.String("error", err.Error()))
		// Continue without tracing - application will use fallback trace IDs
	} else {
		logger.GetLogger().Info(ctx, "OpenTelemetry tracer initialized",
			slog.String("endpoint", cfg.OtelEndpoint),
			slog.String("service", cfg.OtelServiceName))

		// Ensure tracer cleanup on exit
		defer func() {
			if err := shutdownTracer(ctx); err != nil {
				logger.GetLogger().Error(ctx, "Failed to shutdown tracer", slog.String("error", err.Error()))
			} else {
				logger.GetLogger().Info(ctx, "Tracer shutdown successfully")
			}
		}()
	}

	// Initialize OpenTelemetry metrics
	metricsProvider, shutdownMetrics, err := telemetry.InitMetrics(ctx, telemetry.Config{
		ServiceName: cfg.OtelServiceName,
		Endpoint:    cfg.OtelEndpoint,
		Enabled:     cfg.OtelEnabled,
	})
	if err != nil {
		logger.GetLogger().Error(ctx, "Failed to initialize metrics", slog.String("error", err.Error()))
		// Continue without metrics
	} else {
		logger.GetLogger().Info(ctx, "OpenTelemetry metrics initialized",
			slog.String("endpoint", cfg.OtelEndpoint),
			slog.String("service", cfg.OtelServiceName))

		// Ensure metrics cleanup on exit
		defer func() {
			if err := shutdownMetrics(ctx); err != nil {
				logger.GetLogger().Error(ctx, "Failed to shutdown metrics", slog.String("error", err.Error()))
			} else {
				logger.GetLogger().Info(ctx, "Metrics shutdown successfully")
			}
		}()
	}

	clnts, err := clients.NewClients(ctx, cfg)
	if err != nil {
		logger.GetLogger().Fatal(ctx, err.Error())
	}

	// Ensure database cleanup on exit
	defer func() {
		if err := clnts.Db.Close(); err != nil {
			logger.GetLogger().Error(ctx, "Failed to close database connection", slog.String("error", err.Error()))
		} else {
			logger.GetLogger().Info(ctx, "Database connection closed successfully")
		}
	}()

	repos := repositories.NewRepositories(clnts.Db)
	srvs := services.NewServices(repos, cfg)
	mdlwrs := middlewares.NewMiddlewares(srvs, cfg)

	// Create server first to get shutdown flag
	srv := server.NewServer(cfg)
	srv.SetMetricsProvider(metricsProvider) // Pass metrics for shutdown tracking

	// Create handlers with shutdown flag for readiness probe
	hdlrs := handlers.NewHandlers(cfg, srvs, mdlwrs, srv.GetShutdownFlag())

	// Start background jobs (from Services container)
	srvs.JobManager.StartAll(ctx)
	defer srvs.JobManager.StopAll(ctx)

	// Register routes and run server
	srv.RegisterRoutes(hdlrs)
	srv.Run(ctx)
}
