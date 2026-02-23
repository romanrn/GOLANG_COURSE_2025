package handlers

import (
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers/health"
	"football-tracker/cmd/server/middlewares"
	"football-tracker/internal/services"
)

type Handlers struct {
	Health *health.Handler
	Mdlwr  *middlewares.Middlewares
}

func NewHandlers(cfg *config.ServerConfig, svcs *services.Services, mdlwr *middlewares.Middlewares) *Handlers {
	return &Handlers{
		Health: health.NewHandler(cfg),
		Mdlwr:  mdlwr,
	}
}

/*func (h *Handlers) RegisterRoutes(router fiber.Router) {
	router.Get("/health", h.health.Health)

	api := router.Group("/api")
	api.Use(h.mdlwr.Trace.Handle)
	api.Use(h.mdlwr.ErrorHandler.Handle)
	api.Use(h.mdlwr.Logger.Handle)

}*/
