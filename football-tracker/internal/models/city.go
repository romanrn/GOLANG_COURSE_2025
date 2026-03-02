package models

import "time"

// City represents a city where matches are played
type City struct {
	ID          int       `json:"id" db:"id"`
	Name        string    `json:"name" db:"name"`
	CountryCode string    `json:"country_code" db:"country_code"` // ISO 3166-1 alpha-3
	Stadium     *string   `json:"stadium,omitempty" db:"stadium"`
	Timezone    *string   `json:"timezone,omitempty" db:"timezone"` // e.g., "America/New_York"
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
}

// TableName returns the table name for City
func (City) TableName() string {
	return "cities"
}
