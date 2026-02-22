package services

import (
	"context"
	"log/slog"

	"lesson_14/internal/services/dto"
)

type documentService struct {
	logger *slog.Logger
	repo   DocumentRepository
}

func NewDocumentService(logger *slog.Logger, repo DocumentRepository) DocumentService {
	return &documentService{
		logger: logger.With("component", "DocumentService"),
		repo:   repo,
	}
}

func (d documentService) CreateDocument(ctx context.Context, doc *dto.CreateDocumentDTO) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		d.logger.Info("Creating document",
			slog.String("collection", doc.CollectionName),
			slog.Any("document", doc.Document))

		return d.repo.Create(ctx, doc.CollectionName, doc.Document)
	}
}

func (d documentService) ListDocuments(ctx context.Context, collectionName string) ([]*Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		d.logger.Info("Get Document List")
		return d.repo.List(ctx, collectionName)
	}
}

func (d documentService) DeleteDocument(ctx context.Context, collectionName, field, value string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		d.logger.Info("Delete Document by field",
			slog.String("collection", collectionName),
			slog.String("field", field),
			slog.String("value", value))
		return d.repo.Delete(ctx, collectionName, field, value)
	}
}

func (d documentService) GetDocument(ctx context.Context, collectionName, field, value string) (*Document, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		d.logger.Info("Get Document by field",
			slog.String("collection", collectionName),
			slog.String("field", field),
			slog.String("value", value))
		return d.repo.Get(ctx, collectionName, field, value)
	}
}
