package models

// ChampionshipHost represents the many-to-many relationship between championships and host countries
type ChampionshipHost struct {
	ChampionshipID int `json:"championship_id" db:"championship_id"`
	HostCountryID  int `json:"host_country_id" db:"host_country_id"`
}

// TableName returns the table name for ChampionshipHost
func (ChampionshipHost) TableName() string {
	return "championship_hosts"
}

// ChampionshipCity represents the many-to-many relationship between championships and cities
type ChampionshipCity struct {
	ChampionshipID int `json:"championship_id" db:"championship_id"`
	CityID         int `json:"city_id" db:"city_id"`
}

// TableName returns the table name for ChampionshipCity
func (ChampionshipCity) TableName() string {
	return "championship_cities"
}

// ChampionshipTeam represents the many-to-many relationship between championships and teams
type ChampionshipTeam struct {
	ChampionshipID int `json:"championship_id" db:"championship_id"`
	TeamID         int `json:"team_id" db:"team_id"`
}

// TableName returns the table name for ChampionshipTeam
func (ChampionshipTeam) TableName() string {
	return "championship_teams"
}

// TeamGroup represents the relationship between teams and groups for a specific championship
type TeamGroup struct {
	TeamID         int `json:"team_id" db:"team_id"`
	GroupID        int `json:"group_id" db:"group_id"`
	ChampionshipID int `json:"championship_id" db:"championship_id"`
}

// TableName returns the table name for TeamGroup
func (TeamGroup) TableName() string {
	return "team_groups"
}
