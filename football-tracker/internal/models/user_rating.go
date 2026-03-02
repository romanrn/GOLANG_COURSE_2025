package models

import "time"

// UserRating represents global user statistics across all championships
type UserRating struct {
	UserID int `json:"user_id" db:"user_id"` // Primary key

	// Global statistics
	TotalPoints        int     `json:"total_points" db:"total_points"`
	TotalPredictions   int     `json:"total_predictions" db:"total_predictions"`
	TotalChampionships int     `json:"total_championships" db:"total_championships"`
	CorrectScores      int     `json:"correct_scores" db:"correct_scores"`
	CorrectOutcomes    int     `json:"correct_outcomes" db:"correct_outcomes"`
	WrongPredictions   int     `json:"wrong_predictions" db:"wrong_predictions"`
	AccuracyPercentage float64 `json:"accuracy_percentage" db:"accuracy_percentage"`
	AveragePoints      float64 `json:"average_points" db:"average_points"`
	GlobalRank         *int    `json:"global_rank,omitempty" db:"global_rank"`

	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for UserRating
func (UserRating) TableName() string {
	return "user_ratings"
}

// UpdateStats recalculates global statistics
func (r *UserRating) UpdateStats() {
	if r.TotalPredictions > 0 {
		r.AccuracyPercentage = float64(r.CorrectScores+r.CorrectOutcomes) / float64(r.TotalPredictions) * 100
		r.AveragePoints = float64(r.TotalPoints) / float64(r.TotalPredictions)
	} else {
		r.AccuracyPercentage = 0
		r.AveragePoints = 0
	}
}
