package models

import "time"

// Session represents a user session
type Session struct {
	ID        int       `json:"id" db:"id"`
	UserID    int       `json:"user_id" db:"user_id"`
	Token     string    `json:"token" db:"token"`
	ExpiresAt time.Time `json:"expires_at" db:"expires_at"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// TableName returns the table name for Session
func (Session) TableName() string {
	return "sessions"
}

// IsExpired checks if session is expired
func (s *Session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}
