//go:build unit
// +build unit

package services

import (
	"context"
	"errors"
	"football-tracker/internal/models/dto"
	"football-tracker/internal/out/database/mocks"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMatchService_GetByChampionshipId(t *testing.T) {
	tests := []struct {
		name           string
		championshipID int
		mockSetup      func(*mocks.MockMatchRepository)
		expectedLen    int
		expectedError  bool
	}{
		{
			name:           "Success - returns multiple matches",
			championshipID: 1,
			mockSetup: func(m *mocks.MockMatchRepository) {
				matches := []dto.MatchDTO{
					{
						ID:             1,
						ChampionshipID: 1,
						Stage:          "GROUP_STAGE",
						Status:         "SCHEDULED",
						MatchDate:      time.Date(2026, 6, 11, 18, 0, 0, 0, time.UTC),
					},
					{
						ID:             2,
						ChampionshipID: 1,
						Stage:          "GROUP_STAGE",
						Status:         "SCHEDULED",
						MatchDate:      time.Date(2026, 6, 12, 18, 0, 0, 0, time.UTC),
					},
				}
				m.On("GetByChampionshipId", mock.Anything, 1).Return(matches, nil)
			},
			expectedLen:   2,
			expectedError: false,
		},
		{
			name:           "Success - returns empty list",
			championshipID: 2,
			mockSetup: func(m *mocks.MockMatchRepository) {
				m.On("GetByChampionshipId", mock.Anything, 2).Return([]dto.MatchDTO{}, nil)
			},
			expectedLen:   0,
			expectedError: false,
		},
		{
			name:           "Error - repository error",
			championshipID: 1,
			mockSetup: func(m *mocks.MockMatchRepository) {
				m.On("GetByChampionshipId", mock.Anything, 1).Return([]dto.MatchDTO{}, errors.New("database error"))
			},
			expectedLen:   0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockRepo := new(mocks.MockMatchRepository)
			tt.mockSetup(mockRepo)
			service := NewMatchService(mockRepo)
			ctx := context.Background()

			// Act
			matches, err := service.GetByChampionshipId(ctx, tt.championshipID)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, matches, tt.expectedLen)
			}
			mockRepo.AssertExpectations(t)
		})
	}
}

func TestMatchService_GetById(t *testing.T) {
	tests := []struct {
		name           string
		matchID        int
		mockSetup      func(*mocks.MockMatchRepository)
		expectedStatus string
		expectedError  bool
	}{
		{
			name:    "Success - returns match",
			matchID: 1,
			mockSetup: func(m *mocks.MockMatchRepository) {
				match := dto.MatchDTO{
					ID:             1,
					ChampionshipID: 1,
					Stage:          "GROUP_STAGE",
					Status:         "SCHEDULED",
					MatchDate:      time.Date(2026, 6, 11, 18, 0, 0, 0, time.UTC),
				}
				m.On("GetById", mock.Anything, 1).Return(match, nil)
			},
			expectedStatus: "SCHEDULED",
			expectedError:  false,
		},
		{
			name:    "Error - match not found",
			matchID: 999,
			mockSetup: func(m *mocks.MockMatchRepository) {
				m.On("GetById", mock.Anything, 999).Return(dto.MatchDTO{}, errors.New("match not found"))
			},
			expectedStatus: "",
			expectedError:  true,
		},
		{
			name:    "Error - database error",
			matchID: 1,
			mockSetup: func(m *mocks.MockMatchRepository) {
				m.On("GetById", mock.Anything, 1).Return(dto.MatchDTO{}, errors.New("database error"))
			},
			expectedStatus: "",
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockRepo := new(mocks.MockMatchRepository)
			tt.mockSetup(mockRepo)
			service := NewMatchService(mockRepo)
			ctx := context.Background()

			// Act
			match, err := service.GetById(ctx, tt.matchID)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStatus, match.Status)
				assert.Equal(t, tt.matchID, match.ID)
			}
			mockRepo.AssertExpectations(t)
		})
	}
}
