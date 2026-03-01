package handlers

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers/auth"
	"football-tracker/cmd/server/handlers/championships"
	"football-tracker/cmd/server/handlers/health"
	"football-tracker/cmd/server/handlers/matches"
	"football-tracker/cmd/server/handlers/predictions"
	"football-tracker/cmd/server/handlers/teams"
	"football-tracker/cmd/server/middlewares"
	"football-tracker/internal/services"
	"sync/atomic"
)

type Handlers struct {
	Health        *health.Handler
	Mdlwr         *middlewares.Middlewares
	ChampionShips *championships.Handler
	Teams         *teams.Handler
	Matches       *matches.Handler
	Auth          *auth.Handler
	Predictions   *predictions.Handler
}

func NewHandlers(cfg *config.ServerConfig, svcs *services.Services, mdlwr *middlewares.Middlewares, isShuttingDown *atomic.Bool) *Handlers {
	return &Handlers{
		Health:        health.NewHandler(cfg, isShuttingDown),
		Mdlwr:         mdlwr,
		ChampionShips: championships.NewHandler(svcs.Championship),
		Teams:         teams.NewHandler(svcs.Team),
		Matches:       matches.NewHandler(svcs.Match),
		Auth:          auth.NewHandler(svcs.Auth, cfg),
		Predictions:   predictions.NewHandler(svcs.Prediction),
	}
}
