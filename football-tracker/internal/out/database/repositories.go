package repositories

import (
	"football-tracker/internal/clients/database"
)

type Repositories struct {
	MatchRepo        MatchRepository
	ChampionshipRepo ChampionshipRepository
}

func NewRepositories(dbClient *database.PostgresClient) *Repositories {
	return &Repositories{
		MatchRepo:        NewMatchRepository(dbClient),
		ChampionshipRepo: NewChampionshipRepository(dbClient),
	}
}
