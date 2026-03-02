package championships

import (
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/services"
	"log/slog"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	service services.ChampionshipService
}

func NewHandler(service services.ChampionshipService) *Handler {
	return &Handler{
		service: service,
	}
}

// GetAll retrieves all championships
// @Summary      Get all championships
// @Description  Retrieves a list of all available football championships (FIFA World Cup, UEFA Euro, etc.)
// @Tags         Championships
// @Produce      json
// @Success      200 {object} map[string]interface{} "List of championships"
// @Failure      500 {object} map[string]string "Internal server error"
// @Router       /api/v1/championships [get]
func (h *Handler) GetAll(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger.GetLogger().Info(
		ctx,
		"Handling GetAll championships request",
		slog.String("component", "handler"),
		slog.String("method", "GET"),
		slog.String("path", "/api/v1/championships"),
	)

	championships, err := h.service.GetAll(ctx)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get championships",
			slog.String("component", "handler"),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to retrieve championships")
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully handled GetAll championships request",
		slog.String("component", "handler"),
		slog.Int("count", len(championships)),
	)

	return c.JSON(fiber.Map{
		"success": true,
		"data":    championships,
		"count":   len(championships),
	})
}

// GetById retrieves a championship by ID
// @Summary      Get championship by ID
// @Description  Retrieves detailed information about a specific championship
// @Tags         Championships
// @Produce      json
// @Param        id path int true "Championship ID"
// @Success      200 {object} map[string]interface{} "Championship details"
// @Failure      400 {object} map[string]string "Invalid championship ID"
// @Failure      500 {object} map[string]string "Internal server error"
// @Router       /api/v1/championships/{id} [get]
func (h *Handler) GetById(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Get championshipId from path parameter
	championshipIdStr := c.Params("id")
	if championshipIdStr == "" {
		logger.GetLogger().Warn(
			ctx,
			"Missing championshipId path parameter",
			slog.String("component", "handler"),
			slog.String("method", "GET"),
			slog.String("path", "/api/v1/championships/:id"),
		)
		return fiber.NewError(fiber.StatusBadRequest, "championshipId path parameter is required")
	}

	championshipId, err := strconv.Atoi(championshipIdStr)
	if err != nil {
		logger.GetLogger().Warn(
			ctx,
			"Invalid championshipId parameter",
			slog.String("component", "handler"),
			slog.String("championshipId", championshipIdStr),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusBadRequest, "championshipId must be a valid integer")
	}

	logger.GetLogger().Info(
		ctx,
		"Handling GetById championship request",
		slog.String("component", "handler"),
		slog.String("method", "GET"),
		slog.String("path", "/api/v1/championships/:id"),
		slog.Int("championship_id", championshipId),
	)

	championshipById, err := h.service.GetByChampionshipId(ctx, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get championship by ID",
			slog.String("component", "handler"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to retrieve championship")
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully handled GetById championship request",
		slog.String("component", "handler"),
		slog.Int("championship_id", championshipId),
		slog.String("championship_name", championshipById.Name),
	)

	return c.JSON(fiber.Map{
		"success": true,
		"data":    championshipById,
	})
}
