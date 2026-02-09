package handlers

import (
	"lesson_14/internal/server/api"
	"lesson_14/internal/services"
	"lesson_14/internal/services/dto"
	"log/slog"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
)

var validate = validator.New()

type DocumentHandler struct {
	logger  *slog.Logger
	service services.DocumentService
}

func NewDocumentHandler(logger *slog.Logger, service services.DocumentService) *DocumentHandler {
	return &DocumentHandler{
		logger:  logger,
		service: service,
	}
}

func (dh *DocumentHandler) RegisterRoutes(app *fiber.App, logger *slog.Logger) {
	app.Post("/put_document", dh.handleCreateDocument)
	app.Post("/list_documents", dh.handleListDocument)
	app.Post("/delete_document", dh.handleDeleteDocument)
	app.Post("/get_document", dh.handleGetDocument)

}

func (dh *DocumentHandler) handleCreateDocument(c *fiber.Ctx) error {
	var req api.CreateDocumentRequest

	if err := c.BodyParser(&req); err != nil {
		dh.logger.Error("Failed to parse request", "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: 0,
		})
	}
	if err := validate.Struct(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}
	dh.logger.Info("Creating document in collection ", "name", req.CollectionName)

	if err := dh.service.CreateDocument(c.Context(), mapToServiceDTO(&req)); err != nil {
		dh.logger.Error("Failed to create document in ", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}
	return c.Status(fiber.StatusCreated).JSON(api.CreateDeleteResponse{
		Ok:        true,
		Message:   "Document created successfully",
		Timestamp: time.Now().Unix(),
	})
}

func (dh *DocumentHandler) handleListDocument(c *fiber.Ctx) error {
	dh.logger.Info("Getting documents")
	var req api.RetrieveListDocumentRequest

	if err := c.BodyParser(&req); err != nil {
		dh.logger.Error("Failed to parse request", "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(api.ListDocumentsResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.CollectionName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.ListDocumentsResponse{
			Ok:        false,
			Error:     "Collection name is required",
			Timestamp: time.Now().Unix(),
		})
	}

	documents, err := dh.service.ListDocuments(c.Context(), req.CollectionName)
	if err != nil {
		dh.logger.Error("Failed to get documents list", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(api.ListDocumentsResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}
	apiDocuments := make([]*api.Document, len(documents))
	for i, doc := range documents {
		apiDocuments[i] = &api.Document{
			ID:   doc.ID.Hex(),
			Data: doc.Data,
		}
	}
	return c.Status(fiber.StatusOK).JSON(api.ListDocumentsResponse{
		Ok:        true,
		Documents: apiDocuments,
		Timestamp: time.Now().Unix(),
	})
}

func (dh *DocumentHandler) handleDeleteDocument(c *fiber.Ctx) error {
	var req api.RetrieveDeleteDocumentRequest

	if err := c.BodyParser(&req); err != nil {
		dh.logger.Error("Failed to parse request", "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.CollectionName == "" || req.Field == "" || req.Value == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     "Collection name, field and value are required",
			Timestamp: time.Now().Unix(),
		})
	}

	dh.logger.Info("Deleting document",
		"collection", req.CollectionName,
		"field", req.Field,
		"value", req.Value)

	if err := dh.service.DeleteDocument(c.Context(), req.CollectionName, req.Field, req.Value); err != nil {
		dh.logger.Error("Failed to delete document", "error", err)
		return c.Status(fiber.StatusNotFound).JSON(api.CreateDeleteResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(api.CreateDeleteResponse{
		Ok:        true,
		Message:   "Document deleted successfully",
		Timestamp: time.Now().Unix(),
	})
}

func (dh *DocumentHandler) handleGetDocument(c *fiber.Ctx) error {
	var req api.RetrieveDeleteDocumentRequest

	if err := c.BodyParser(&req); err != nil {
		dh.logger.Error("Failed to parse request", "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(api.GetDocumentResponse{
			Ok:        false,
			Error:     "Invalid request body",
			Timestamp: time.Now().Unix(),
		})
	}

	if req.CollectionName == "" || req.Field == "" || req.Value == "" {
		return c.Status(fiber.StatusBadRequest).JSON(api.GetDocumentResponse{
			Ok:        false,
			Error:     "Collection name, field and value are required",
			Timestamp: time.Now().Unix(),
		})
	}
	document, err := dh.service.GetDocument(c.Context(), req.CollectionName, req.Field, req.Value)
	if err != nil {
		dh.logger.Error("Failed to get document", "error", err)
		return c.Status(fiber.StatusNotFound).JSON(api.GetDocumentResponse{
			Ok:        false,
			Error:     err.Error(),
			Timestamp: time.Now().Unix(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(api.GetDocumentResponse{
		Ok: true,
		Document: &api.Document{
			ID:   document.ID.Hex(),
			Data: document.Data,
		},
		Timestamp: time.Now().Unix(),
	})
}

func mapToServiceDTO(req *api.CreateDocumentRequest) *dto.CreateDocumentDTO {
	return &dto.CreateDocumentDTO{
		CollectionName: req.CollectionName,
		Document:       req.Document,
	}
}
