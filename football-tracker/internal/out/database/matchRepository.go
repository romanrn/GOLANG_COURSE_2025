package repositories

import (
	"context"
	"football-tracker/internal/clients/database"
)

type matchRepository struct {
}

func NewMatchRepository(dbClient *database.PostgresClient) MatchRepository {
	return &matchRepository{}
}

func (m *matchRepository) GetByID(ctx context.Context, id string) (string, error) {
	//TODO implement me
	panic("implement me")
}
