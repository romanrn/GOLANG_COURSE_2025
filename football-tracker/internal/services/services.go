package services

import (
	repositories "football-tracker/internal/out/database"
)

type Services struct {
	Match        MatchService
	Championship ChampionshipService
}

func NewServices(repos *repositories.Repositories) *Services {
	return &Services{
		Match:        NewMatchService(repos.MatchRepo),
		Championship: NewChampionshipService(repos.ChampionshipRepo),
	}
}
