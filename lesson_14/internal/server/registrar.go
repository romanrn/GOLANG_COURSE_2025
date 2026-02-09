package server

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

type HandlerRegistrar interface {
	RegisterRoutes(app *fiber.App, logger *slog.Logger)
}
