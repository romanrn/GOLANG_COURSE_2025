package teams

import (
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/services"
	"log/slog"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	service services.TeamService
}

func NewHandler(service services.TeamService) *Handler {
	return &Handler{
		service: service,
	}
}

// GetByChampionshipId retrieves teams by championship ID
// @Summary      Get teams by championship
// @Description  Retrieves all teams participating in a specific championship
// @Tags         Teams
// @Produce      json
// @Param        championshipId query int true "Championship ID"
// @Success      200 {object} map[string]interface{} "List of teams"
// @Failure      400 {object} map[string]string "Missing or invalid championshipId"
// @Failure      500 {object} map[string]string "Internal server error"
// @Router       /api/v1/teams [get]
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
			slog.String("path", "/api/v1/teams"),
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
		"Handling GetByChampionshipId teams request",
		slog.String("component", "handler"),
		slog.String("method", "GET"),
		slog.String("path", "/api/v1/teams"),
		slog.Int("championship_id", championshipId),
	)

	teams, err := h.service.GetByChampionshipId(ctx, championshipId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get teams",
			slog.String("component", "handler"),
			slog.Int("championship_id", championshipId),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to retrieve teams")
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully handled GetByChampionshipId teams request",
		slog.String("component", "handler"),
		slog.Int("championship_id", championshipId),
		slog.Int("count", len(teams)),
	)

	return c.JSON(fiber.Map{
		"success": true,
		"data":    teams,
		"count":   len(teams),
	})
}

// GetById retrieves a team by ID
// @Summary      Get team by ID
// @Description  Retrieves detailed information about a specific team
// @Tags         Teams
// @Produce      json
// @Param        id path int true "Team ID"
// @Success      200 {object} map[string]interface{} "Team details"
// @Failure      400 {object} map[string]string "Invalid team ID"
// @Failure      500 {object} map[string]string "Internal server error"
// @Router       /api/v1/teams/{id} [get]
func (h *Handler) GetById(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Get teamId from path parameter
	teamIdStr := c.Params("id")
	if teamIdStr == "" {
		logger.GetLogger().Warn(
			ctx,
			"Missing teamId path parameter",
			slog.String("component", "handler"),
			slog.String("method", "GET"),
			slog.String("path", "/api/v1/teams/:id"),
		)
		return fiber.NewError(fiber.StatusBadRequest, "teamId path parameter is required")
	}

	teamId, err := strconv.Atoi(teamIdStr)
	if err != nil {
		logger.GetLogger().Warn(
			ctx,
			"Invalid teamId parameter",
			slog.String("component", "handler"),
			slog.String("teamId", teamIdStr),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusBadRequest, "teamId must be a valid integer")
	}

	logger.GetLogger().Info(
		ctx,
		"Handling GetById team request",
		slog.String("component", "handler"),
		slog.String("method", "GET"),
		slog.String("path", "/api/v1/teams/:id"),
		slog.Int("team_id", teamId),
	)

	team, err := h.service.GetById(ctx, teamId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get team by ID",
			slog.String("component", "handler"),
			slog.Int("team_id", teamId),
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to retrieve team")
	}

	logger.GetLogger().Info(
		ctx,
		"Successfully handled GetById team request",
		slog.String("component", "handler"),
		slog.Int("team_id", teamId),
		slog.String("team_name", team.Name),
	)

	return c.JSON(fiber.Map{
		"success": true,
		"data":    team,
	})
}
