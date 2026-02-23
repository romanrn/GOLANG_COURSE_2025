package repositories

import "context"

type MatchRepository interface {
	GetByID(ctx context.Context, id string) (string, error)
}
