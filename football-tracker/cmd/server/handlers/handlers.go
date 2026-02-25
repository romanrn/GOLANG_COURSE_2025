package handlers

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers/championships"
	"football-tracker/cmd/server/handlers/health"
	"football-tracker/cmd/server/handlers/teams"
	"football-tracker/cmd/server/middlewares"
	"football-tracker/internal/services"
)

type Handlers struct {
	Health        *health.Handler
	Mdlwr         *middlewares.Middlewares
	ChampionShips *championships.Handler
	Teams         *teams.Handler
}

func NewHandlers(cfg *config.ServerConfig, svcs *services.Services, mdlwr *middlewares.Middlewares) *Handlers {
	return &Handlers{
		Health:        health.NewHandler(cfg),
		Mdlwr:         mdlwr,
		ChampionShips: championships.NewHandler(svcs.Championship),
		Teams:         teams.NewHandler(svcs.Team),
	}
}
