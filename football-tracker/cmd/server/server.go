package server

import (
	"context"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/telemetry"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
)

type Server struct {
	app             *fiber.App
	config          *config.ServerConfig
	metricsProvider *telemetry.MetricsProvider
	isShuttingDown  atomic.Bool // Track shutdown state for readiness probe
}

func NewServer(config *config.ServerConfig) *Server {
	app := fiber.New(fiber.Config{
		AppName:      config.AppName,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	server := &Server{
		app:    app,
		config: config,
	}

	return server
}

// SetMetricsProvider sets the metrics provider for the server
func (s *Server) SetMetricsProvider(provider *telemetry.MetricsProvider) {
	s.metricsProvider = provider
}

// GetShutdownFlag returns pointer to shutdown flag for health checks
func (s *Server) GetShutdownFlag() *atomic.Bool {
	return &s.isShuttingDown
}

func (s *Server) Start() error {
	addr := s.config.Host + ":" + s.config.Port
	return s.app.Listen(addr)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.app.ShutdownWithContext(ctx)
}

func (s *Server) Run(ctx context.Context) {
	errChan := make(chan error, 1)
	// Start server in a goroutine
	go func() {

		logger.GetLogger().Info(
			ctx,
			"Starting server",
			slog.String("host", s.config.Host),
			slog.String("port", s.config.Port),
		)

		if err := s.Start(); err != nil {
			errChan <- err
		}
	}()

	// Create channel to listen for interrupt signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		logger.GetLogger().Error(
			ctx,
			"Failed to start server",
			slog.String("error", err.Error()),
		)

		os.Exit(1)
	case <-quit:
		logger.GetLogger().Info(ctx, "Shutting down server...")
		s.isShuttingDown.Store(true) // Mark server as not ready
		logger.GetLogger().Info(ctx, "Readiness flag set to false - /ready will return 503")

	case <-ctx.Done():
		logger.GetLogger().Info(ctx, "Context cancelled, shutting down")
		s.isShuttingDown.Store(true) // Mark server as not ready
		logger.GetLogger().Info(ctx, "Readiness flag set to false - /ready will return 503")
	}

	// Track shutdown duration for metrics
	shutdownStart := time.Now()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	shutdownErr := s.Shutdown(shutdownCtx)
	shutdownDuration := time.Since(shutdownStart)

	if shutdownErr != nil {
		// Record failed shutdown metrics
		if s.metricsProvider != nil {
			s.metricsProvider.RecordShutdown(ctx, shutdownDuration, false)
		}

		logger.GetLogger().Error(
			ctx,
			"Server forced to shutdown",
			slog.String("error", shutdownErr.Error()),
			slog.Duration("shutdown_duration", shutdownDuration),
		)
	} else {
		// Record successful shutdown metrics
		if s.metricsProvider != nil {
			s.metricsProvider.RecordShutdown(ctx, shutdownDuration, true)
		}

		// Log successful shutdown with duration metric
		logger.GetLogger().Info(
			context.Background(),
			"Server graceful shutdown completed",
			slog.Duration("shutdown_duration", shutdownDuration),
			slog.Duration("shutdown_timeout", s.config.ShutdownTimeout),
		)
	}

	logger.GetLogger().Info(context.Background(), "Server exited")
}

func (s *Server) RegisterRoutes(h *handlers.Handlers) {

	s.app.Get("/healthCheck", h.Health.Check)
	s.app.Get("/ready", h.Health.Ready)

	api := s.app.Group("/api/v1")

	// Apply global middlewares to all routes
	// IMPORTANT ORDER: OTEL FIRST (generates trace_id and creates span)
	api.Use(h.Mdlwr.Otel.Handle)         // 1. Create span (auto-generates trace_id) and send to Tempo
	api.Use(h.Mdlwr.ErrorHandler.Handle) // 2. Error handling
	api.Use(h.Mdlwr.Logger.Handle)       // 3. Request/response logging (uses trace_id from context)

	// Auth routes (public endpoints - no authentication required)
	auth := api.Group("/auth")
	auth.Post("/register", h.Auth.Register)
	auth.Post("/login", h.Auth.Login)

	// Protected auth routes (require authentication)
	authProtected := api.Group("/auth")
	authProtected.Use(h.Mdlwr.Auth.Handle) // Apply Auth middleware
	authProtected.Post("/logout", h.Auth.Logout)

	// Public routes with optional authentication (logs user_id if authenticated)
	public := api.Group("")
	public.Use(h.Mdlwr.Auth.OptionalHandle) // Optional Auth: logs user_id if present

	// Championships routes (public, but logs user_id if authenticated)
	public.Get("/championships", h.ChampionShips.GetAll)
	public.Get("/championships/:id", h.ChampionShips.GetById)

	// Teams routes (public, but logs user_id if authenticated)
	public.Get("/teams", h.Teams.GetByChampionshipId)
	public.Get("/teams/:id", h.Teams.GetById)

	// Matches routes (public, but logs user_id if authenticated)
	public.Get("/matches", h.Matches.GetByChampionshipId)
	public.Get("/matches/:id", h.Matches.GetById)

	// Protected prediction routes (require authentication)
	predictions := api.Group("/predictions")
	predictions.Use(h.Mdlwr.Auth.Handle) // Require authentication

	predictions.Post("", h.Predictions.CreatePrediction) // Create prediction
	
}
