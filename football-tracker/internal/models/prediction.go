package models

import "time"

// Prediction represents a user's prediction for a match
type Prediction struct {
	ID      int `json:"id" db:"id"`
	UserID  int `json:"user_id" db:"user_id"`
	MatchID int `json:"match_id" db:"match_id"`

	// Regular time prediction (required for all matches)
	HomeScoreRegular int `json:"home_score_regular" db:"home_score_regular"`
	AwayScoreRegular int `json:"away_score_regular" db:"away_score_regular"`

	// Total score prediction (optional, for playoff matches)
	HomeScoreTotal *int `json:"home_score_total,omitempty" db:"home_score_total"`
	AwayScoreTotal *int `json:"away_score_total,omitempty" db:"away_score_total"`

	// Penalty shootout winner prediction (optional, for playoff matches)
	PenaltyWinnerTeamID *int `json:"penalty_winner_team_id,omitempty" db:"penalty_winner_team_id"`

	Points    *int      `json:"points,omitempty" db:"points"` // Calculated points
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for Prediction
func (Prediction) TableName() string {
	return "predictions"
}

// PredictionPoints constants for scoring
const (
	PointsExactScore      = 3 // Exact score in regular time
	PointsCorrectOutcome  = 1 // Correct outcome (win/draw/loss)
	PointsWrongPrediction = 0 // Wrong prediction
	BonusExactTotal       = 1 // Bonus for exact total score in playoff
	BonusPenaltyWinner    = 1 // Bonus for correct penalty winner
)

// CalculatePoints calculates points based on prediction and actual match result
func (p *Prediction) CalculatePoints(match *Match) int {
	if match.HomeScoreRegular == nil || match.AwayScoreRegular == nil {
		return 0 // Match not finished yet
	}

	points := 0

	// Check regular time prediction
	if p.HomeScoreRegular == *match.HomeScoreRegular && p.AwayScoreRegular == *match.AwayScoreRegular {
		points += PointsExactScore // Exact score
	} else if p.getOutcome(p.HomeScoreRegular, p.AwayScoreRegular) == p.getOutcome(*match.HomeScoreRegular, *match.AwayScoreRegular) {
		points += PointsCorrectOutcome // Correct outcome
	}

	// Bonus for exact total score (playoff matches with extra time)
	if match.HasExtraTime && p.HomeScoreTotal != nil && match.HomeScoreTotal != nil {
		if *p.HomeScoreTotal == *match.HomeScoreTotal && *p.AwayScoreTotal == *match.AwayScoreTotal {
			points += BonusExactTotal
		}
	}

	// Bonus for correct penalty winner (playoff matches with penalties)
	// Note: This requires winner_team_id field in matches table or separate penalty_shootouts table
	// For now, this is a placeholder
	if match.HasPenalty && p.PenaltyWinnerTeamID != nil {
		// TODO: Implement when penalty winner tracking is added
		// points += BonusPenaltyWinner
	}

	return points
}

// getOutcome returns the outcome: 1 for home win, 0 for draw, -1 for away win
func (p *Prediction) getOutcome(homeScore, awayScore int) int {
	if homeScore > awayScore {
		return 1
	} else if homeScore < awayScore {
		return -1
	}
	return 0
}
