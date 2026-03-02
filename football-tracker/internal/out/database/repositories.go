package repositories

import (
	"football-tracker/internal/clients/database"
)

type Repositories struct {
	MatchRepo        MatchRepository
	ChampionshipRepo ChampionshipRepository
	TeamRepo         TeamRepository
	UserRepo         UserRepository
	SessionRepo      SessionRepository
	PredictionRepo   PredictionRepository
}

func NewRepositories(dbClient *database.PostgresClient) *Repositories {
	return &Repositories{
		MatchRepo:        NewMatchRepository(dbClient),
		ChampionshipRepo: NewChampionshipRepository(dbClient),
		TeamRepo:         NewTeamRepository(dbClient),
		UserRepo:         NewUserRepository(dbClient),
		SessionRepo:      NewSessionRepository(dbClient),
		PredictionRepo:   NewPredictionRepository(dbClient.DB),
	}
}
