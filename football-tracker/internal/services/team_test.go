//go:build unit
// +build unit

package services

import (
	"context"
	"errors"
	"football-tracker/internal/models"
	"football-tracker/internal/out/database/mocks"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTeamService_GetByChampionshipId(t *testing.T) {
	tests := []struct {
		name           string
		championshipID int
		mockSetup      func(*mocks.MockTeamRepository)
		expectedLen    int
		expectedError  bool
	}{
		{
			name:           "Success - returns multiple teams",
			championshipID: 1,
			mockSetup: func(m *mocks.MockTeamRepository) {
				teams := []models.Team{
					{
						ID:          1,
						Name:        "Brazil",
						CountryCode: "BRA",
					},
					{
						ID:          2,
						Name:        "Argentina",
						CountryCode: "ARG",
					},
				}
				m.On("GetByChampionshipId", mock.Anything, 1).Return(teams, nil)
			},
			expectedLen:   2,
			expectedError: false,
		},
		{
			name:           "Success - returns empty list",
			championshipID: 2,
			mockSetup: func(m *mocks.MockTeamRepository) {
				m.On("GetByChampionshipId", mock.Anything, 2).Return([]models.Team{}, nil)
			},
			expectedLen:   0,
			expectedError: false,
		},
		{
			name:           "Error - repository error",
			championshipID: 1,
			mockSetup: func(m *mocks.MockTeamRepository) {
				m.On("GetByChampionshipId", mock.Anything, 1).Return([]models.Team{}, errors.New("database error"))
			},
			expectedLen:   0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockRepo := new(mocks.MockTeamRepository)
			tt.mockSetup(mockRepo)
			service := NewTeamService(mockRepo)
			ctx := context.Background()

			// Act
			teams, err := service.GetByChampionshipId(ctx, tt.championshipID)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, teams, tt.expectedLen)
			}
			mockRepo.AssertExpectations(t)
		})
	}
}

func TestTeamService_GetById(t *testing.T) {
	tests := []struct {
		name          string
		teamID        int
		mockSetup     func(*mocks.MockTeamRepository)
		expectedName  string
		expectedError bool
	}{
		{
			name:   "Success - returns team",
			teamID: 1,
			mockSetup: func(m *mocks.MockTeamRepository) {
				team := models.Team{
					ID:          1,
					Name:        "Brazil",
					CountryCode: "BRA",
				}
				m.On("GetById", mock.Anything, 1).Return(team, nil)
			},
			expectedName:  "Brazil",
			expectedError: false,
		},
		{
			name:   "Error - team not found",
			teamID: 999,
			mockSetup: func(m *mocks.MockTeamRepository) {
				m.On("GetById", mock.Anything, 999).Return(models.Team{}, errors.New("team not found"))
			},
			expectedName:  "",
			expectedError: true,
		},
		{
			name:   "Error - database error",
			teamID: 1,
			mockSetup: func(m *mocks.MockTeamRepository) {
				m.On("GetById", mock.Anything, 1).Return(models.Team{}, errors.New("database error"))
			},
			expectedName:  "",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockRepo := new(mocks.MockTeamRepository)
			tt.mockSetup(mockRepo)
			service := NewTeamService(mockRepo)
			ctx := context.Background()

			// Act
			team, err := service.GetById(ctx, tt.teamID)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedName, team.Name)
				assert.Equal(t, tt.teamID, team.ID)
			}
			mockRepo.AssertExpectations(t)
		})
	}
}
