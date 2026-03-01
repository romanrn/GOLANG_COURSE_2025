package health

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"log/slog"
	"sync/atomic"

	"github.com/gofiber/fiber/v2"
)

type Response struct {
	Status string `json:"status"`
	Env    string `json:"env"`
}

type Handler struct {
	cfg            *config.ServerConfig
	isShuttingDown *atomic.Bool // Shared shutdown state from server
}

func NewHandler(cfg *config.ServerConfig, isShuttingDown *atomic.Bool) *Handler {
	return &Handler{
		cfg:            cfg,
		isShuttingDown: isShuttingDown,
	}
}

func (h *Handler) Check(c *fiber.Ctx) error {
	return c.JSON(&Response{
		Status: "ok",
		Env:    h.cfg.Enviroment,
	})
}

// Ready returns 200 if server is ready to accept requests, 503 if shutting down
func (h *Handler) Ready(c *fiber.Ctx) error {
	if h.isShuttingDown != nil && h.isShuttingDown.Load() {
		logger.GetLogger().Warn(
			c.Context(),
			"Readiness check failed - server is shutting down",
			slog.String("component", "handler"),
			slog.String("method", "Ready"),
		)
		return c.Status(fiber.StatusServiceUnavailable).JSON(&Response{
			Status: "shutting down",
			Env:    h.cfg.Enviroment,
		})
	}

	return c.JSON(&Response{
		Status: "ready",
		Env:    h.cfg.Enviroment,
	})
}
