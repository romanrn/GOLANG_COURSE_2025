package models

import "time"

// Championship represents a football championship/tournament
type Championship struct {
	ID        int       `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Year      int       `json:"year" db:"year"`
	StartDate time.Time `json:"start_date" db:"start_date"`
	EndDate   time.Time `json:"end_date" db:"end_date"`
	LogoURL   *string   `json:"logo_url,omitempty" db:"logo_url"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for Championship
func (Championship) TableName() string {
	return "championships"
}
