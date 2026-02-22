package handlers

import (
	"lesson_14/internal/server/api"
	"lesson_14/internal/services"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
)

type CollectionHandler struct {
	appName string
	logger  *slog.Logger
	service services.CollectionService
}

func NewCollectionHandler(appName string, logger *slog.Logger, service services.CollectionService) *CollectionHandler {
	return &CollectionHandler{
		appName: appName,
		logger:  logger,
		service: service,
	}
}

func (ch *CollectionHandler) RegisterRoutes(app *fiber.App, logger *slog.Logger) {
	app.Post("/create_collection", ch.handleCreateCollection)
	app.Post("/list_collections", ch.handleListCollection)
	app.Post("/delete_collection", ch.handleDeleteCollection)

}

func (ch *CollectionHandler) handleCreateCollection(c *fiber.Ctx) error {

	var req api.CreateDeleteCollectionRequest

	if err := c.BodyParser(&req); err != nil {
		ch.logger.Error("Failed to parse request", "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Collection name is required",
			Timestamp: time.Now().Unix(),
		})
	}

	ch.logger.Info("Creating collection", "name", req.Name)

	if err := ch.service.CreateCollection(c.Context(), req.Name); err != nil {
		ch.logger.Error("Failed to create collection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(api.CreateDeleteResponse{
		Ok:         true,
		Message:    "Collection created successfully",
		Collection: req.Name,
		Timestamp:  time.Now().Unix(),
	})
}

func (ch *CollectionHandler) handleListCollection(c *fiber.Ctx) error {

	ch.logger.Info("Getting collections")
	collections, err := ch.service.ListCollection(c.Context())
	if err != nil {
		ch.logger.Error("Failed to create collection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(api.ListCollectionResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}

	apiCollections := make([]*api.Collection, 0, len(collections))
	for i, col := range collections {
		apiCollections = append(apiCollections, &api.Collection{
			ID:   i + 1,
			Name: col.Name,
		})
	}

	return c.Status(fiber.StatusOK).JSON(api.ListCollectionResponse{
		Ok:          true,
		Message:     "Retrieve Collections successfully",
		Collections: apiCollections,
		Timestamp:   time.Now().Unix(),
	})
}

func (ch *CollectionHandler) handleDeleteCollection(c *fiber.Ctx) error {

	var req api.CreateDeleteCollectionRequest

	if err := c.BodyParser(&req); err != nil {
		ch.logger.Error("Failed to parse request", "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Collection name is required",
			Timestamp: time.Now().Unix(),
		})
	}

	ch.logger.Info("Deleting collection", "name", req.Name)

	if err := ch.service.DeleteCollection(c.Context(), req.Name); err != nil {
		ch.logger.Error("Failed to delete collection", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(api.CreateDeleteResponse{
		Ok:         true,
		Message:    "Collection deleted successfully",
		Collection: req.Name,
		Timestamp:  time.Now().Unix(),
	})
}
