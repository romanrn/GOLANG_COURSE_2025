package predictions

import (
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/models/dto"
	"football-tracker/internal/services"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	predictionService services.PredictionService
}

func NewHandler(predictionService services.PredictionService) *Handler {
	return &Handler{
		predictionService: predictionService,
	}
}

// CreatePrediction creates a new prediction for a match (POST /api/v1/predictions)
func (h *Handler) CreatePrediction(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Get user ID from context (set by auth middleware)
	userID, ok := ctx.Value("user_id").(int)
	if !ok {
		logger.GetLogger().Error(ctx, "User ID not found in context",
			slog.String("component", "handler"),
			slog.String("method", "CreatePrediction"))
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "user not authenticated",
		})
	}

	// Parse request body
	var req dto.CreatePredictionRequest
	if err := c.BodyParser(&req); err != nil {
		logger.GetLogger().Warn(ctx, "Failed to parse request body",
			slog.String("component", "handler"),
			slog.String("method", "CreatePrediction"),
			slog.String("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}

	logger.GetLogger().Info(ctx, "Creating prediction",
		slog.String("component", "handler"),
		slog.String("method", "CreatePrediction"),
		slog.Int("user_id", userID),
		slog.Int("match_id", req.MatchID))

	// Create prediction
	prediction, err := h.predictionService.CreatePrediction(ctx, userID, &req)
	if err != nil {
		logger.GetLogger().Error(ctx, "Failed to create prediction",
			slog.String("component", "handler"),
			slog.String("method", "CreatePrediction"),
			slog.String("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	logger.GetLogger().Info(ctx, "Successfully created prediction",
		slog.String("component", "handler"),
		slog.String("method", "CreatePrediction"),
		slog.Int("prediction_id", prediction.ID))

	return c.Status(fiber.StatusCreated).JSON(prediction)
}
