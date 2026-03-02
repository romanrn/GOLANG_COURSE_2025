package models

import "time"

// Match represents a football match
type Match struct {
	ID             int       `json:"id" db:"id"`
	ChampionshipID int       `json:"championship_id" db:"championship_id"`
	GroupID        *int      `json:"group_id,omitempty" db:"group_id"` // NULL for playoff matches
	HomeTeamID     int       `json:"home_team_id" db:"home_team_id"`
	AwayTeamID     int       `json:"away_team_id" db:"away_team_id"`
	CityID         *int      `json:"city_id,omitempty" db:"city_id"`
	MatchDate      time.Time `json:"match_date" db:"match_date"`

	// Score breakdown
	HomeScoreRegular *int `json:"home_score_regular,omitempty" db:"home_score_regular"` // Score in regular time (90 minutes)
	AwayScoreRegular *int `json:"away_score_regular,omitempty" db:"away_score_regular"` // Score in regular time (90 minutes)
	HomeScoreTotal   *int `json:"home_score_total,omitempty" db:"home_score_total"`     // Total score (regular + extra)
	AwayScoreTotal   *int `json:"away_score_total,omitempty" db:"away_score_total"`     // Total score (regular + extra)

	// Match progression flags
	HasExtraTime bool `json:"has_extra_time" db:"has_extra_time"` // Whether match went to extra time
	HasPenalty   bool `json:"has_penalty" db:"has_penalty"`       // Whether match went to penalty shootout

	Status    string    `json:"status" db:"status"` // SC, LV, FN, CN, PP
	Stage     string    `json:"stage" db:"stage"`   // GR, 16, 32, QF, SF, FN, TP
	Venue     *string   `json:"venue,omitempty" db:"venue"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for Match
func (Match) TableName() string {
	return "matches"
}

// MatchStatus constants
const (
	MatchStatusScheduled = "SC" // Scheduled
	MatchStatusLive      = "LV" // Live
	MatchStatusFinished  = "FN" // Finished
	MatchStatusCancelled = "CN" // Cancelled
	MatchStatusPostponed = "PP" // Postponed
)

// MatchStage constants
const (
	MatchStageGroup        = "GR" // Group Stage
	MatchStageRoundOf32    = "32" // Round of 32
	MatchStageRoundOf16    = "16" // Round of 16
	MatchStageQuarterFinal = "QF" // Quarter Finals
	MatchStageSemiFinal    = "SF" // Semi Finals
	MatchStageThirdPlace   = "TP" // Third Place
	MatchStageFinal        = "FN" // Final
)

// GetExtraTimeScore calculates the score during extra time
func (m *Match) GetExtraTimeScore() (homeExtra, awayExtra int) {
	if !m.HasExtraTime || m.HomeScoreRegular == nil || m.HomeScoreTotal == nil {
		return 0, 0
	}
	return *m.HomeScoreTotal - *m.HomeScoreRegular, *m.AwayScoreTotal - *m.AwayScoreRegular
}

// IsGroupStage checks if match is in group stage
func (m *Match) IsGroupStage() bool {
	return m.Stage == MatchStageGroup
}

// IsPlayoff checks if match is in playoff stage
func (m *Match) IsPlayoff() bool {
	return !m.IsGroupStage()
}
