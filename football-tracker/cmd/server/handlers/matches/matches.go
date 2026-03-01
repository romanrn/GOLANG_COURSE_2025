package matches

import (
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/services"
	"log/slog"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	service services.MatchService
}

func NewHandler(service services.MatchService) *Handler {
	return &Handler{
		service: service,
	}
}

func (h *Handler) GetByChampionshipId(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Get championshipId from query parameter
	championshipIdStr := c.Query("championshipId")
	if championshipIdStr == "" {
		logger.GetLogger().Warn(
			ctx,
			"Missing championshipId query parameter",
			slog.String("component", "handler"),
			slog.String("method", "GET"),
			slog.String("path", "/api/v1/matches"),
		)
		return fiber.NewError(fiber.StatusBadRequest, "championshipId query parameter is required")
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
		"Handling GetByChampionshipId matches request",
		slog.String("component", "handler"),
		slog.String("method", "GET"),
		slog.String("path", "/api/v1/matches"),
		slog.Int("championship_id", championshipId),
	)

	matches, err := h.service.GetByChampionshipId(ctx, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get matches",
			slog.String("component", "handler"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to retrieve matches")
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully handled GetByChampionshipId matches request",
		slog.String("component", "handler"),
		slog.Int("championship_id", championshipId),
		slog.Int("count", len(matches)),
	)

	return c.JSON(fiber.Map{
		"success": true,
		"data":    matches,
		"count":   len(matches),
	})
}

func (h *Handler) GetById(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Get matchId from path parameter
	matchIdStr := c.Params("id")
	if matchIdStr == "" {
		logger.GetLogger().Warn(
			ctx,
			"Missing matchId path parameter",
			slog.String("component", "handler"),
			slog.String("method", "GET"),
			slog.String("path", "/api/v1/matches/:id"),
		)
		return fiber.NewError(fiber.StatusBadRequest, "matchId path parameter is required")
	}

	matchId, err := strconv.Atoi(matchIdStr)
	if err != nil {
		logger.GetLogger().Warn(
			ctx,
			"Invalid matchId parameter",
			slog.String("component", "handler"),
			slog.String("matchId", matchIdStr),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusBadRequest, "matchId must be a valid integer")
	}

	logger.GetLogger().Info(
		ctx,
		"Handling GetById match request",
		slog.String("component", "handler"),
		slog.String("method", "GET"),
		slog.String("path", "/api/v1/matches/:id"),
		slog.Int("match_id", matchId),
	)

	match, err := h.service.GetById(ctx, matchId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get match by ID",
			slog.String("component", "handler"),
			slog.Int("match_id", matchId),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to retrieve match")
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully handled GetById match request",
		slog.String("component", "handler"),
		slog.Int("match_id", matchId),
		slog.String("status", match.Status),
		slog.String("stage", match.Stage),
	)

	return c.JSON(fiber.Map{
		"success": true,
		"data":    match,
	})
}
