package models

import "time"

// HostCountry represents a country hosting a championship
type HostCountry struct {
	ID        int       `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Code      string    `json:"code" db:"code"` // ISO 3166-1 alpha-3
	FlagURL   *string   `json:"flag_url,omitempty" db:"flag_url"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// TableName returns the table name for HostCountry
func (HostCountry) TableName() string {
	return "host_countries"
}
