package clients

import (
	"context"
	"football-tracker/cmd/server/config"
	"football-tracker/internal/clients/database"
)

type Clients struct {
	Db *database.PostgresClient
}

func NewClients(ctx context.Context, cfg *config.ServerConfig) (*Clients, error) {
	db, err := database.NewPostgresClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Clients{
		Db: db,
	}, nil
}
