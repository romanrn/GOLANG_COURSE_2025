package handlers

import (
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
)

type HealthHandler struct {
	appName string
}

func NewHealthHandler(appName string) *HealthHandler {
	return &HealthHandler{
		appName: appName,
	}
}

func (h *HealthHandler) RegisterRoutes(app *fiber.App, logger *slog.Logger) {
	check := app.Group("/check")

	check.Get("/health", h.handleHealth)
	check.Get("/ready", h.handleReady)
}

func (h *HealthHandler) handleHealth(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "ok",
		"timestamp": time.Now().Unix(),
	})
}

func (h *HealthHandler) handleReady(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "ready",
		"app_name":  h.appName,
		"timestamp": time.Now().Unix(),
	})
}

/*
func (s *Server) setupRoutes() {


	s.app.Post("/create_collection", s.handleCreateCollection)
}*/
