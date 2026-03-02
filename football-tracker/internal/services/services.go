package services

import (
	"context"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/jobs"
	repositories "football-tracker/internal/out/database"
	"log/slog"
)

type Services struct {
	Match        MatchService
	Championship ChampionshipService
	Team         TeamService
	Auth         AuthService
	Prediction   PredictionService
	JobManager   *jobs.Manager
}

func NewServices(repos *repositories.Repositories, cfg *config.ServerConfig) *Services {
	// Initialize job manager
	jobManager := jobs.NewManager()

	// Register jobs based on configuration
	if cfg.JobsEnabled {
		if cfg.JobSessionCleanupEnabled {
			jobManager.Register(jobs.NewSessionCleaner(repos.SessionRepo, cfg.SessionCleanupInterval))
			logger.GetLogger().Info(context.Background(), "Session cleanup job registered",
				slog.String("component", "services"),
				slog.Duration("interval", cfg.SessionCleanupInterval))
		} else {
			logger.GetLogger().Info(context.Background(), "Session cleanup job disabled by configuration",
				slog.String("component", "services"))
		}

		// Future jobs can be registered here conditionally:
		// if cfg.JobMatchCleanupEnabled {
		//     jobManager.Register(jobs.NewMatchCleaner(repos.MatchRepo, cfg.MatchCleanupInterval))
		// }
	} else {
		logger.GetLogger().Info(context.Background(), "All background jobs disabled by configuration",
			slog.String("component", "services"))
	}

	return &Services{
		Match:        NewMatchService(repos.MatchRepo),
		Championship: NewChampionshipService(repos.ChampionshipRepo),
		Team:         NewTeamService(repos.TeamRepo),
		Auth:         NewAuthService(repos.UserRepo, repos.SessionRepo, cfg.SessionTokenTTL),
		Prediction:   NewPredictionService(repos.PredictionRepo, repos.MatchRepo),
		JobManager:   jobManager,
	}
}
