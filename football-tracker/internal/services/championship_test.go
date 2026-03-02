//go:build unit
// +build unit

package services

import (
	"context"
	"errors"
	"football-tracker/internal/models"
	"football-tracker/internal/out/database/mocks"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestChampionshipService_GetAll(t *testing.T) {
	tests := []struct {
		name          string
		mockSetup     func(*mocks.MockChampionshipRepository)
		expectedLen   int
		expectedError bool
	}{
		{
			name: "Success - returns multiple championships",
			mockSetup: func(m *mocks.MockChampionshipRepository) {
				championships := []models.Championship{
					{
						ID:        1,
						Name:      "FIFA World Cup 2026",
						Year:      2026,
						StartDate: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC),
						EndDate:   time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:        2,
						Name:      "UEFA Euro 2024",
						Year:      2024,
						StartDate: time.Date(2024, 6, 14, 0, 0, 0, 0, time.UTC),
						EndDate:   time.Date(2024, 7, 14, 0, 0, 0, 0, time.UTC),
					},
				}
				m.On("GetAll", mock.Anything).Return(championships, nil)
			},
			expectedLen:   2,
			expectedError: false,
		},
		{
			name: "Success - returns empty list",
			mockSetup: func(m *mocks.MockChampionshipRepository) {
				m.On("GetAll", mock.Anything).Return([]models.Championship{}, nil)
			},
			expectedLen:   0,
			expectedError: false,
		},
		{
			name: "Error - repository error",
			mockSetup: func(m *mocks.MockChampionshipRepository) {
				m.On("GetAll", mock.Anything).Return([]models.Championship{}, errors.New("database error"))
			},
			expectedLen:   0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockRepo := new(mocks.MockChampionshipRepository)
			tt.mockSetup(mockRepo)
			service := NewChampionshipService(mockRepo)
			ctx := context.Background()

			// Act
			championships, err := service.GetAll(ctx)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, championships, tt.expectedLen)
			}
			mockRepo.AssertExpectations(t)
		})
	}
}

func TestChampionshipService_GetByChampionshipId(t *testing.T) {
	tests := []struct {
		name           string
		championshipId int
		mockSetup      func(*mocks.MockChampionshipRepository)
		expectedName   string
		expectedError  bool
	}{
		{
			name:           "Success - returns championship",
			championshipId: 1,
			mockSetup: func(m *mocks.MockChampionshipRepository) {
				championship := models.Championship{
					ID:        1,
					Name:      "FIFA World Cup 2026",
					Year:      2026,
					StartDate: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC),
					EndDate:   time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
				}
				m.On("GetByChampionshipId", mock.Anything, 1).Return(championship, nil)
			},
			expectedName:  "FIFA World Cup 2026",
			expectedError: false,
		},
		{
			name:           "Error - championship not found",
			championshipId: 999,
			mockSetup: func(m *mocks.MockChampionshipRepository) {
				m.On("GetByChampionshipId", mock.Anything, 999).Return(models.Championship{}, errors.New("championship not found"))
			},
			expectedName:  "",
			expectedError: true,
		},
		{
			name:           "Error - database error",
			championshipId: 1,
			mockSetup: func(m *mocks.MockChampionshipRepository) {
				m.On("GetByChampionshipId", mock.Anything, 1).Return(models.Championship{}, errors.New("database error"))
			},
			expectedName:  "",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockRepo := new(mocks.MockChampionshipRepository)
			tt.mockSetup(mockRepo)
			service := NewChampionshipService(mockRepo)
			ctx := context.Background()

			// Act
			championship, err := service.GetByChampionshipId(ctx, tt.championshipId)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedName, championship.Name)
				assert.Equal(t, tt.championshipId, championship.ID)
			}
			mockRepo.AssertExpectations(t)
		})
	}
}
