package main

import (
	"context"
	"football-tracker/cmd/server"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/handlers"
	"football-tracker/cmd/server/logger"
	"football-tracker/cmd/server/middlewares"
	"football-tracker/internal/clients"
	"football-tracker/internal/out/database"
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

	clnts, err := clients.NewClients(ctx, cfg)
	if err != nil {
		logger.GetLogger().Fatal(ctx, err.Error())
	}

	// Ensure database cleanup on exit
	defer func() {
		if err := clnts.Db.Close(); err != nil {
			logger.GetLogger().Error(ctx, "Failed to close database connection", slog.String("error", err.Error()))
		} else {
			logger.GetLogger().Info(ctx, "Database connection closed successfully")
		}
	}()

	// mdlwrs := middlewares.NewMiddlewares(cfg, clnts, svcs)
	repos := repositories.NewRepositories(clnts.Db)
	srvs := services.NewServices(repos, cfg)
	mdlwrs := middlewares.NewMiddlewares(srvs, cfg)

	hdlrs := handlers.NewHandlers(cfg, srvs, mdlwrs)

	// Create server
	// srv := server.NewServer(config, logger, hdlrs)
	srv := server.NewServer(cfg)
	srv.RegisterRoutes(hdlrs)

	// Run server
	srv.Run(ctx)
}
