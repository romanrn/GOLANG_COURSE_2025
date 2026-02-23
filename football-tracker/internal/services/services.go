package services

import (
	"football-tracker/cmd/server/config"
	"football-tracker/internal/clients"
)

type Services struct {
	Match MatchService
}

func NewServices(cfg *config.ServerConfig, clients *clients.Clients) *Services {
	return &Services{
		Match: NewMatchService(),
	}
}
