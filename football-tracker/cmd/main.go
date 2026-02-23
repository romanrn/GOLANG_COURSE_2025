package main

import (
	"context"
	"football-tracker/cmd/server"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers"
	"football-tracker/cmd/server/logger"
	"football-tracker/cmd/server/middlewares"
	"football-tracker/internal/clients"
	"football-tracker/internal/services"
	"log/slog"
)

func main() {
	ctx := context.Background()

	cfg, err := config.NewConfigFromEnv()
	if err != nil {
		slog.ErrorContext(ctx, err.Error())
		panic(err)
	}

	logger.InitLogger(cfg)

	clients, err := clients.NewClients(ctx, cfg)
	if err != nil {
		logger.GetLogger().Fatal(ctx, err.Error())
	}

	// mdlwrs := middlewares.NewMiddlewares(cfg, clnts, svcs)
	srvs := services.NewServices(cfg, clients)
	mdlwrs := middlewares.NewMiddlewares(cfg)

	hdlrs := handlers.NewHandlers(cfg, srvs, mdlwrs)

	// Create server
	// srv := server.NewServer(config, logger, hdlrs)
	srv := server.NewServer(cfg)
	srv.RegisterRoutes(hdlrs)

	// Run server
	srv.Run(ctx)
}
