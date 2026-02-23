package services

import "context"

type MatchService interface {
	GetByID(ctx context.Context, id string) (string, error)
}
