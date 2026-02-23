package repositories

import (
	"football-tracker/internal/clients/database"
)

type Repositories struct {
	MatchRepo MatchRepository
}

func NewRepositories(dbClient *database.PostgresClient) *Repositories {
	return &Repositories{
		MatchRepo: NewMatchRepository(dbClient),
	}
}
