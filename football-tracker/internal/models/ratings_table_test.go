//go:build unit
// +build unit

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserRating_UpdateStats_WithPredictions(t *testing.T) {
	rating := &UserRating{
		TotalPoints:      17,
		TotalPredictions: 10,
		CorrectScores:    3,
		CorrectOutcomes:  2,
	}

	rating.UpdateStats()

	assert.InDelta(t, 50.0, rating.AccuracyPercentage, 0.0001)
	assert.InDelta(t, 1.7, rating.AveragePoints, 0.0001)
}

func TestUserRating_UpdateStats_NoPredictions(t *testing.T) {
	rating := &UserRating{}

	rating.UpdateStats()

	assert.Equal(t, 0.0, rating.AccuracyPercentage)
	assert.Equal(t, 0.0, rating.AveragePoints)
}

func TestChampionshipUserRating_UpdateStats_WithPredictions(t *testing.T) {
	rating := &ChampionshipUserRating{
		TotalPoints:      9,
		TotalPredictions: 4,
		CorrectScores:    1,
		CorrectOutcomes:  2,
	}

	rating.UpdateStats()

	assert.InDelta(t, 75.0, rating.AccuracyPercentage, 0.0001)
	assert.InDelta(t, 2.25, rating.AveragePoints, 0.0001)
}

func TestChampionshipUserRating_UpdateStats_NoPredictions(t *testing.T) {
	rating := &ChampionshipUserRating{}

	rating.UpdateStats()

	assert.Equal(t, 0.0, rating.AccuracyPercentage)
	assert.Equal(t, 0.0, rating.AveragePoints)
}

func TestAdditionalTableNames(t *testing.T) {
	assert.Equal(t, "cities", City{}.TableName())
	assert.Equal(t, "groups", Group{}.TableName())
	assert.Equal(t, "predictions", Prediction{}.TableName())
	assert.Equal(t, "user_ratings", UserRating{}.TableName())
	assert.Equal(t, "championship_user_ratings", ChampionshipUserRating{}.TableName())
	assert.Equal(t, "championship_hosts", ChampionshipHost{}.TableName())
	assert.Equal(t, "championship_cities", ChampionshipCity{}.TableName())
	assert.Equal(t, "championship_teams", ChampionshipTeam{}.TableName())
	assert.Equal(t, "team_groups", TeamGroup{}.TableName())
}
