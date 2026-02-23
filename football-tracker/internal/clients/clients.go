package clients

import (
	"context"
	"football-tracker/cmd/server/config"
)

type Clients struct {
}

func NewClients(ctx context.Context, cfg *config.ServerConfig) (*Clients, error) {
	/*mongo, err := app_mongo.NewMongo(ctx, cfg)
	if err != nil {
		return nil, err
	}*/
	return &Clients{}, nil
}
