package server

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
)

type Server struct {
	app    *fiber.App
	config *ServerConfig
	logger *slog.Logger
}

type ServerConfig struct {
	Host            string
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	AppName         string
	DbType          string
	DbName          string
	DbHost          string
	DbPort          string
	DbUri           string
	DbUserName      string
	DbPsw           string
	MaxPoolSize     uint64
	MinPoolSize     uint64
	MaxConnIdleTime time.Duration
}

func NewServer(config *ServerConfig, logger *slog.Logger, registrars ...HandlerRegistrar) *Server {
	app := fiber.New(fiber.Config{
		AppName:      config.AppName,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	server := &Server{
		app:    app,
		config: config,
		logger: logger,
	}

	for _, registrar := range registrars {
		registrar.RegisterRoutes(server.app, server.logger)
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

func (s *Server) Run() {
	errChan := make(chan error, 1)
	// Start server in a goroutine
	go func() {
		s.logger.Info("Starting server",
			"host", s.config.Host, "port", s.config.Port)
		if err := s.Start(); err != nil {
			errChan <- err
		}
	}()

	// Create channel to listen for interrupt signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		s.logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	case <-quit:
		s.logger.Info("Shutting down server...")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		s.logger.Error("Server forced to shutdown", "error", err)
	}

	s.logger.Info("Server exited")
}
