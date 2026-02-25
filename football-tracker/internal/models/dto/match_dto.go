package dto

import (
	"fmt"
	"time"
)

// MatchDTO represents a match with full team details for API responses
type MatchDTO struct {
	ID             int       `json:"id"`
	ChampionshipID int       `json:"championship_id"`
	GroupID        *int      `json:"group_id"`
	MatchDate      time.Time `json:"match_date"`

	// Home Team Full Details
	HomeTeam TeamBasic `json:"home_team"`

	// Away Team Full Details
	AwayTeam TeamBasic `json:"away_team"`

	// Match Result - always present, null for scheduled matches
	Result *MatchResult `json:"result"`

	// Match Info
	HasExtraTime bool      `json:"has_extra_time"`
	HasPenalty   bool      `json:"has_penalty"`
	Status       string    `json:"status"`
	Stage        string    `json:"stage"`
	Venue        *string   `json:"venue,omitempty"`
	CityID       *int      `json:"city_id"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// TeamBasic represents basic team information for nested responses
type TeamBasic struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	CountryCode string  `json:"country_code"`
	CountryName string  `json:"country_name"`
	FlagURL     *string `json:"flag_url,omitempty"`
}

// MatchResult represents the score of a match
type MatchResult struct {
	HomeScoreRegular *int   `json:"home_score_regular"`
	AwayScoreRegular *int   `json:"away_score_regular"`
	HomeScoreTotal   *int   `json:"home_score_total"`
	AwayScoreTotal   *int   `json:"away_score_total"`
	Score            string `json:"score"` // Formatted score: "0:0" or "0:0 (1:2)" for extra time
}

// FormatScore creates a formatted score string
func FormatScore(homeRegular, awayRegular, homeTotal, awayTotal *int, hasExtraTime bool) string {
	// Default score
	homeReg := 0
	awayReg := 0

	if homeRegular != nil {
		homeReg = *homeRegular
	}
	if awayRegular != nil {
		awayReg = *awayRegular
	}

	regularScore := fmt.Sprintf("%d:%d", homeReg, awayReg)

	// If has extra time, add total score in parentheses
	if hasExtraTime && homeTotal != nil && awayTotal != nil {
		return fmt.Sprintf("%s (%d:%d)", regularScore, *homeTotal, *awayTotal)
	}

	return regularScore
}
