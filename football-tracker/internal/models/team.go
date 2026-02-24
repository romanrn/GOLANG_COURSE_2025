package models

import "time"

// Team represents a national football team
type Team struct {
	ID          int       `json:"id" db:"id"`
	Name        string    `json:"name" db:"name"`                 // e.g., "Argentina National Team"
	CountryCode string    `json:"country_code" db:"country_code"` // ISO 3166-1 alpha-3: ARG, BRA, etc.
	CountryName string    `json:"country_name" db:"country_name"` // Full country name for display
	FlagURL     *string   `json:"flag_url,omitempty" db:"flag_url"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for Team
func (Team) TableName() string {
	return "teams"
}
