package mocks

import (
	"context"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"

	"github.com/stretchr/testify/mock"
)

// getValueOrZero safely extracts a value from mock arguments.
// Returns zero value if the argument is nil, preventing panic on type assertion.
func getValueOrZero[T any](args mock.Arguments, index int) T {
	if args.Get(index) == nil {
		var zero T
		return zero
	}
	return args.Get(index).(T)
}

// MockChampionshipRepository is a mock implementation of ChampionshipRepository
type MockChampionshipRepository struct {
	mock.Mock
}

func (m *MockChampionshipRepository) GetAll(ctx context.Context) ([]models.Championship, error) {
	args := m.Called(ctx)
	return getValueOrZero[[]models.Championship](args, 0), args.Error(1)
}

func (m *MockChampionshipRepository) GetByChampionshipId(ctx context.Context, championshipId int) (models.Championship, error) {
	args := m.Called(ctx, championshipId)
	return getValueOrZero[models.Championship](args, 0), args.Error(1)
}

// MockTeamRepository is a mock implementation of TeamRepository
type MockTeamRepository struct {
	mock.Mock
}

func (m *MockTeamRepository) GetByChampionshipId(ctx context.Context, championshipId int) ([]models.Team, error) {
	args := m.Called(ctx, championshipId)
	return getValueOrZero[[]models.Team](args, 0), args.Error(1)
}

func (m *MockTeamRepository) GetById(ctx context.Context, teamId int) (models.Team, error) {
	args := m.Called(ctx, teamId)
	return getValueOrZero[models.Team](args, 0), args.Error(1)
}

// MockMatchRepository is a mock implementation of MatchRepository
type MockMatchRepository struct {
	mock.Mock
}

func (m *MockMatchRepository) GetByChampionshipId(ctx context.Context, championshipId int) ([]dto.MatchDTO, error) {
	args := m.Called(ctx, championshipId)
	return getValueOrZero[[]dto.MatchDTO](args, 0), args.Error(1)
}

func (m *MockMatchRepository) GetById(ctx context.Context, matchId int) (dto.MatchDTO, error) {
	args := m.Called(ctx, matchId)
	return getValueOrZero[dto.MatchDTO](args, 0), args.Error(1)
}

// MockUserRepository is a mock implementation of UserRepository
type MockUserRepository struct {
	mock.Mock
}

func (m *MockUserRepository) Create(ctx context.Context, user *models.User) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserRepository) GetByUsername(ctx context.Context, username string) (*models.User, error) {
	args := m.Called(ctx, username)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.User), args.Error(1)
}

func (m *MockUserRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	args := m.Called(ctx, email)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.User), args.Error(1)
}

func (m *MockUserRepository) GetById(ctx context.Context, userId int) (*models.User, error) {
	args := m.Called(ctx, userId)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.User), args.Error(1)
}

// MockSessionRepository is a mock implementation of SessionRepository
type MockSessionRepository struct {
	mock.Mock
}

func (m *MockSessionRepository) Create(ctx context.Context, session *models.Session) error {
	args := m.Called(ctx, session)
	return args.Error(0)
}

func (m *MockSessionRepository) GetByToken(ctx context.Context, token string) (*models.Session, error) {
	args := m.Called(ctx, token)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Session), args.Error(1)
}

func (m *MockSessionRepository) DeleteByToken(ctx context.Context, token string) error {
	args := m.Called(ctx, token)
	return args.Error(0)
}

func (m *MockSessionRepository) DeleteByUserId(ctx context.Context, userId int) error {
	args := m.Called(ctx, userId)
	return args.Error(0)
}

func (m *MockSessionRepository) DeleteExpired(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockPredictionRepository is a mock implementation of PredictionRepository
type MockPredictionRepository struct {
	mock.Mock
}

func (m *MockPredictionRepository) CreatePrediction(ctx context.Context, prediction *models.Prediction) error {
	args := m.Called(ctx, prediction)
	return args.Error(0)
}

func (m *MockPredictionRepository) GetPredictionByUserAndMatch(ctx context.Context, userID, matchID int) (*models.Prediction, error) {
	args := m.Called(ctx, userID, matchID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Prediction), args.Error(1)
}
