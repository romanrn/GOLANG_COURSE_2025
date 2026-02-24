package models

import "time"

// MatchWithDetails represents a match with all related information
type MatchWithDetails struct {
	Match

	// Related entities
	Championship *Championship `json:"championship,omitempty"`
	Group        *Group        `json:"group,omitempty"`
	HomeTeam     *Team         `json:"home_team,omitempty"`
	AwayTeam     *Team         `json:"away_team,omitempty"`
	City         *City         `json:"city,omitempty"`
}

// PredictionWithDetails represents a prediction with match and user details
type PredictionWithDetails struct {
	Prediction

	// Related entities
	User   *UserResponse     `json:"user,omitempty"`
	Match  *MatchWithDetails `json:"match,omitempty"`
	Winner *Team             `json:"penalty_winner,omitempty"`
}

// LeaderboardEntry represents a user's position in the leaderboard
type LeaderboardEntry struct {
	Rank               int     `json:"rank"`
	UserID             int     `json:"user_id"`
	Username           string  `json:"username"`
	TotalPoints        int     `json:"total_points"`
	TotalPredictions   int     `json:"total_predictions"`
	CorrectScores      int     `json:"correct_scores"`
	CorrectOutcomes    int     `json:"correct_outcomes"`
	AccuracyPercentage float64 `json:"accuracy_percentage"`
	AveragePoints      float64 `json:"average_points"`
}

// GroupStandings represents team standings in a group
type GroupStandings struct {
	GroupID   int            `json:"group_id"`
	GroupName string         `json:"group_name"`
	Teams     []TeamStanding `json:"teams"`
}

// TeamStanding represents a team's standing in a group
type TeamStanding struct {
	TeamID         int    `json:"team_id"`
	TeamName       string `json:"team_name"`
	CountryCode    string `json:"country_code"`
	MatchesPlayed  int    `json:"matches_played"`
	Wins           int    `json:"wins"`
	Draws          int    `json:"draws"`
	Losses         int    `json:"losses"`
	GoalsFor       int    `json:"goals_for"`
	GoalsAgainst   int    `json:"goals_against"`
	GoalDifference int    `json:"goal_difference"`
	Points         int    `json:"points"` // 3 for win, 1 for draw, 0 for loss
}

// MatchResult represents a formatted match result for display
type MatchResult struct {
	MatchID   int       `json:"match_id"`
	MatchDate time.Time `json:"match_date"`
	HomeTeam  string    `json:"home_team"`
	AwayTeam  string    `json:"away_team"`
	HomeFlag  string    `json:"home_flag"`
	AwayFlag  string    `json:"away_flag"`

	// Scores
	RegularScore string  `json:"regular_score"`         // e.g., "2-1"
	TotalScore   *string `json:"total_score,omitempty"` // e.g., "3-2 (AET)"

	// Match info
	Status       string `json:"status"`
	Stage        string `json:"stage"`
	Venue        string `json:"venue"`
	HasExtraTime bool   `json:"has_extra_time"`
	HasPenalty   bool   `json:"has_penalty"`
}

// UserStats represents comprehensive user statistics
type UserStats struct {
	UserID              int                      `json:"user_id"`
	Username            string                   `json:"username"`
	GlobalRating        *UserRating              `json:"global_rating,omitempty"`
	ChampionshipRatings []ChampionshipUserRating `json:"championship_ratings,omitempty"`
	RecentPredictions   []PredictionWithDetails  `json:"recent_predictions,omitempty"`
}

// ChampionshipDetails represents a championship with all related data
type ChampionshipDetails struct {
	Championship

	HostCountries []HostCountry `json:"host_countries,omitempty"`
	Cities        []City        `json:"cities,omitempty"`
	Teams         []Team        `json:"teams,omitempty"`
	Groups        []Group       `json:"groups,omitempty"`
	MatchesCount  int           `json:"matches_count"`
	TeamsCount    int           `json:"teams_count"`
}
