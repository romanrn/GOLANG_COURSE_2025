package handlers

import (
	"lesson_14/internal/server/api"
	"lesson_14/internal/services"
	"lesson_14/internal/services/dto"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
)

type IndexHandler struct {
	logger  *slog.Logger
	service services.IndexService
}

func NewIndexHandler(logger *slog.Logger, service services.IndexService) *IndexHandler {
	return &IndexHandler{
		logger:  logger,
		service: service,
	}
}

func (ih *IndexHandler) RegisterRoutes(app *fiber.App, logger *slog.Logger) {
	app.Post("/create_index", ih.handleCreateIndex)
	app.Post("/delete_index", ih.handleDeleteIndex)

}

func (ih *IndexHandler) handleCreateIndex(c *fiber.Ctx) error {

	ctx := c.Context()

	var req api.CreateIndexRequest
	if err := c.BodyParser(&req); err != nil {
		ih.logger.Error("Failed to parse request body", slog.String("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	if req.CollectionName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "collection_name is required",
		})
	}

	if len(req.Fields) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "at least one field is required",
		})
	}

	for i, field := range req.Fields {
		if field.Name == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "field name cannot be empty",
				"index": i,
			})
		}
		if field.Direction != 1 && field.Direction != -1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "direction must be 1 (ascending) or -1 (descending)",
				"field": field.Name,
			})
		}
	}

	dtoFields := make([]dto.IndexField, len(req.Fields))
	for i, field := range req.Fields {
		dtoFields[i] = dto.IndexField{
			Name:      field.Name,
			Direction: field.Direction,
		}
	}

	serviceDTO := &dto.CreateIndexDTO{
		CollectionName: req.CollectionName,
		Fields:         dtoFields,
		Unique:         req.Unique,
		Name:           req.Name,
	}

	if err := ih.service.CreateIndex(ctx, serviceDTO); err != nil {
		ih.logger.Error("Failed to create index",
			slog.String("collection", req.CollectionName),
			slog.String("error", err.Error()))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create index",
		})
	}

	ih.logger.Info("Index created successfully",
		slog.String("collection", req.CollectionName),
		slog.Int("fieldsCount", len(req.Fields)))

	response := api.CreateIndexResponse{
		Message:    "Index created successfully",
		Collection: req.CollectionName,
		Fields:     req.Fields,
		Unique:     req.Unique,
	}

	return c.Status(fiber.StatusCreated).JSON(response)
}

func (ih *IndexHandler) handleDeleteIndex(c *fiber.Ctx) error {
	ctx := c.Context()

	var req api.DeleteIndexRequest
	if err := c.BodyParser(&req); err != nil {
		ih.logger.Error("Failed to parse request body", slog.String("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.CollectionName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "collection_name is required",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "name is required",
			Timestamp: time.Now().Unix(),
		})
	}

	if err := ih.service.DeleteIndex(ctx, req.CollectionName, req.Name); err != nil {
		ih.logger.Error("Failed to delete index",
			slog.String("collection", req.CollectionName),
			slog.String("indexName", req.Name),
			slog.String("error", err.Error()))
		return c.Status(fiber.StatusInternalServerError).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Failed to delete index",
			Timestamp: time.Now().Unix(),
		})
	}

	ih.logger.Info("Index deleted successfully",
		slog.String("collection", req.CollectionName),
		slog.String("indexName", req.Name))

	return c.Status(fiber.StatusOK).JSON(api.CreateDeleteResponse{
		Ok:         true,
		Message:    "Index deleted successfully",
		Collection: req.CollectionName,
		Timestamp:  time.Now().Unix(),
	})
}
