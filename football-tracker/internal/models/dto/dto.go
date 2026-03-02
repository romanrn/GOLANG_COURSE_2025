package dto

import "football-tracker/internal/models"

// MatchWithDetails represents a match with all related information
type MatchWithDetails struct {
	models.Match

	// Related entities
	Championship *models.Championship `json:"championship,omitempty"`
	Group        *models.Group        `json:"group,omitempty"`
	HomeTeam     *models.Team         `json:"home_team,omitempty"`
	AwayTeam     *models.Team         `json:"away_team,omitempty"`
	City         *models.City         `json:"city,omitempty"`
}

// PredictionWithDetails represents a prediction with match and user details
type PredictionWithDetails struct {
	models.Prediction

	// Related entities
	User   *models.UserResponse `json:"user,omitempty"`
	Match  *MatchWithDetails    `json:"match,omitempty"`
	Winner *models.Team         `json:"penalty_winner,omitempty"`
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

// UserStats represents comprehensive user statistics
type UserStats struct {
	UserID              int                             `json:"user_id"`
	Username            string                          `json:"username"`
	GlobalRating        *models.UserRating              `json:"global_rating,omitempty"`
	ChampionshipRatings []models.ChampionshipUserRating `json:"championship_ratings,omitempty"`
	RecentPredictions   []PredictionWithDetails         `json:"recent_predictions,omitempty"`
}

// ChampionshipDetails represents a championship with all related data
type ChampionshipDetails struct {
	models.Championship

	HostCountries []models.HostCountry `json:"host_countries,omitempty"`
	Cities        []models.City        `json:"cities,omitempty"`
	Teams         []models.Team        `json:"teams,omitempty"`
	Groups        []models.Group       `json:"groups,omitempty"`
	MatchesCount  int                  `json:"matches_count"`
	TeamsCount    int                  `json:"teams_count"`
}
