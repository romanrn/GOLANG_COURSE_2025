package services

import (
	"football-tracker/cmd/server/config"
	repositories "football-tracker/internal/out/database"
)

type Services struct {
	Match        MatchService
	Championship ChampionshipService
	Team         TeamService
	Auth         AuthService
}

func NewServices(repos *repositories.Repositories, cfg *config.ServerConfig) *Services {
	return &Services{
		Match:        NewMatchService(repos.MatchRepo),
		Championship: NewChampionshipService(repos.ChampionshipRepo),
		Team:         NewTeamService(repos.TeamRepo),
		Auth:         NewAuthService(repos.UserRepo, repos.SessionRepo, cfg.SessionTokenTTL),
	}
}
