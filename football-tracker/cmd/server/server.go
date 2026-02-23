package server

import (
	"context"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers"
	"football-tracker/cmd/server/logger"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
)

type Server struct {
	app    *fiber.App
	config *config.ServerConfig
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

	case <-ctx.Done():
		logger.GetLogger().Info(ctx, "Context cancelled, shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.Shutdown(shutdownCtx); err != nil {

		logger.GetLogger().Error(
			ctx,
			"Server forced to shutdown",
			slog.String("error", err.Error()),
		)
	}

	logger.GetLogger().Info(context.Background(), "Server exited")
}

func (s *Server) RegisterRoutes(h *handlers.Handlers) {

	s.app.Get("/health", h.Health.Check)

	api := s.app.Group("/api")

	api.Use(h.Mdlwr.Trace.Handle)
	api.Use(h.Mdlwr.ErrorHandler.Handle)
	api.Use(h.Mdlwr.Logger.Handle)
}
