//go:build unit
// +build unit

package services

import (
	"context"
	"errors"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"
	"football-tracker/internal/out/database/mocks"
	"football-tracker/internal/testhelpers"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPredictionService_CreatePrediction(t *testing.T) {
	now := time.Now()
	futureDate := now.Add(48 * time.Hour)
	pastDate := now.Add(-48 * time.Hour)

	tests := []struct {
		name               string
		userID             int
		request            *dto.CreatePredictionRequest
		mockPredictionRepo func(*mocks.MockPredictionRepository)
		mockMatchRepo      func(*mocks.MockMatchRepository)
		expectedError      bool
		errorContains      string
	}{
		{
			name:   "Success - create prediction for future match",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(nil, nil)
				m.On("CreatePrediction", mock.Anything, mock.AnythingOfType("*models.Prediction")).Return(nil)
			},
			expectedError: false,
		},
		{
			name:   "Success - create prediction with total scores",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 1,
				AwayScoreRegular: 1,
				HomeScoreTotal:   testhelpers.IntPtr(2),
				AwayScoreTotal:   testhelpers.IntPtr(1),
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(nil, nil)
				m.On("CreatePrediction", mock.Anything, mock.AnythingOfType("*models.Prediction")).Return(nil)
			},
			expectedError: false,
		},
		{
			name:   "Error - match not found",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          999,
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				m.On("GetById", mock.Anything, 999).Return(dto.MatchDTO{}, errors.New("match not found"))
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {},
			expectedError:      true,
			errorContains:      "failed to get match",
		},
		{
			name:   "Error - match already started",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: pastDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {},
			expectedError:      true,
			errorContains:      "already started",
		},
		{
			name:   "Error - prediction already exists",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				existingPrediction := &models.Prediction{
					ID:      1,
					UserID:  1,
					MatchID: 1,
				}
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(existingPrediction, nil)
			},
			expectedError: true,
			errorContains: "already exists",
		},
		{
			name:   "Error - incomplete total scores (only home)",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 1,
				AwayScoreRegular: 1,
				HomeScoreTotal:   testhelpers.IntPtr(2),
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(nil, nil)
			},
			expectedError: true,
			errorContains: "both home_score_total and away_score_total must be provided together",
		},
		{
			name:   "Error - incomplete total scores (only away)",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 1,
				AwayScoreRegular: 1,
				AwayScoreTotal:   testhelpers.IntPtr(1),
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(nil, nil)
			},
			expectedError: true,
			errorContains: "both home_score_total and away_score_total must be provided together",
		},
		{
			name:   "Error - repository create fails",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(nil, nil)
				m.On("CreatePrediction", mock.Anything, mock.AnythingOfType("*models.Prediction")).Return(errors.New("database error"))
			},
			expectedError: true,
			errorContains: "failed to create prediction",
		},
		{
			name:   "Error - failed to check existing prediction",
			userID: 1,
			request: &dto.CreatePredictionRequest{
				MatchID:          1,
				HomeScoreRegular: 2,
				AwayScoreRegular: 1,
			},
			mockMatchRepo: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:        1,
					MatchDate: futureDate,
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			mockPredictionRepo: func(m *mocks.MockPredictionRepository) {
				m.On("GetPredictionByUserAndMatch", mock.Anything, 1, 1).Return(nil, errors.New("lookup failed"))
			},
			expectedError: true,
			errorContains: "failed to check existing prediction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockPredictionRepo := new(mocks.MockPredictionRepository)
			mockMatchRepo := new(mocks.MockMatchRepository)
			tt.mockMatchRepo(mockMatchRepo)
			tt.mockPredictionRepo(mockPredictionRepo)

			service := NewPredictionService(mockPredictionRepo, mockMatchRepo)
			ctx := context.Background()

			// Act
			response, err := service.CreatePrediction(ctx, tt.userID, tt.request)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.Equal(t, tt.userID, response.UserID)
				assert.Equal(t, tt.request.MatchID, response.MatchID)
				assert.Equal(t, tt.request.HomeScoreRegular, response.HomeScoreRegular)
				assert.Equal(t, tt.request.AwayScoreRegular, response.AwayScoreRegular)
			}
			mockMatchRepo.AssertExpectations(t)
			mockPredictionRepo.AssertExpectations(t)
		})
	}
}
