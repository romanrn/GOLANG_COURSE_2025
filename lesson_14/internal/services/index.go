package services

import (
	"context"
	"log/slog"

	"lesson_14/internal/services/dto"
)

type indexService struct {
	logger *slog.Logger
	repo   IndexRepository
}

func NewIndexService(logger *slog.Logger, repo IndexRepository) IndexService {
	return &indexService{
		logger: logger.With("component", "IndexService"),
		repo:   repo,
	}
}

func (s *indexService) CreateIndex(ctx context.Context, dto *dto.CreateIndexDTO) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.logger.Info("Creating index",
			slog.String("collection", dto.CollectionName),
			slog.Int("fieldsCount", len(dto.Fields)),
			slog.Bool("unique", dto.Unique),
			slog.String("name", dto.Name))

		if err := s.repo.Create(ctx, dto.CollectionName, dto.Fields, dto.Unique, dto.Name); err != nil {
			s.logger.Error("Failed to create index", slog.String("error", err.Error()))
			return err
		}

		return nil
	}
}

func (i indexService) DeleteIndex(ctx context.Context, collectionName, indexName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		i.logger.Info("Deleting index",
			slog.String("collection", collectionName),
			slog.String("indexName", indexName))

		if err := i.repo.Delete(ctx, collectionName, indexName); err != nil {
			i.logger.Error("Failed to delete index",
				slog.String("collection", collectionName),
				slog.String("indexName", indexName),
				slog.String("error", err.Error()))
			return err
		}

		i.logger.Info("Index deleted successfully",
			slog.String("collection", collectionName),
			slog.String("indexName", indexName))

		return nil
	}
}
