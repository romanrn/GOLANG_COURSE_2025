package models

// Group represents a tournament group (A, B, C, etc.)
type Group struct {
	ID             int    `json:"id" db:"id"`
	Name           string `json:"name" db:"name"` // A, B, C, D, E, F, G, H
	ChampionshipID int    `json:"championship_id" db:"championship_id"`
}

// TableName returns the table name for Group
func (Group) TableName() string {
	return "groups"
}
