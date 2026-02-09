package services

import (
	"context"
	"log/slog"
)

type collectionService struct {
	logger *slog.Logger
	repo   CollectionRepository
}

func NewCollectionService(logger *slog.Logger, repo CollectionRepository) CollectionService {
	return &collectionService{
		logger: logger.With("component", "CollectionService"),
		repo:   repo,
	}
}

func (s *collectionService) CreateCollection(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.logger.Info("Creating collection", "name", name)
		return s.repo.Create(ctx, name)
	}
}

func (s *collectionService) ListCollection(ctx context.Context) ([]*Collection, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		s.logger.Info("Get Collection List")
		return s.repo.List(ctx)
	}
}

func (s *collectionService) DeleteCollection(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.logger.Info("Deleting collection", "name", name)
		return s.repo.Delete(ctx, name)
	}
}
