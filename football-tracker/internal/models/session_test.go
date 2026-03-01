//go:build unit
// +build unit

package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSession_IsExpired(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		session  Session
		expected bool
	}{
		{
			name: "Session not expired - expires in future",
			session: Session{
				ID:        1,
				UserID:    1,
				Token:     "valid_token",
				ExpiresAt: now.Add(24 * time.Hour),
				CreatedAt: now.Add(-1 * time.Hour),
			},
			expected: false,
		},
		{
			name: "Session expired - expired long ago",
			session: Session{
				ID:        2,
				UserID:    1,
				Token:     "expired_token",
				ExpiresAt: now.Add(-24 * time.Hour),
				CreatedAt: now.Add(-48 * time.Hour),
			},
			expected: true,
		},
		{
			name: "Session not expired - long TTL",
			session: Session{
				ID:        3,
				UserID:    1,
				Token:     "long_lived_token",
				ExpiresAt: now.Add(7 * 24 * time.Hour),
				CreatedAt: now,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			isExpired := tt.session.IsExpired()

			// Assert
			assert.Equal(t, tt.expected, isExpired)
		})
	}
}

func TestSession_TableName(t *testing.T) {
	session := Session{}
	tableName := session.TableName()
	assert.Equal(t, "sessions", tableName)
}
