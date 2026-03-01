//go:build unit
// +build unit

package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUser_IsAdmin(t *testing.T) {
	tests := []struct {
		name     string
		user     User
		expected bool
	}{
		{
			name: "Admin user returns true",
			user: User{
				ID:       1,
				Username: "admin",
				Role:     UserRoleAdmin,
			},
			expected: true,
		},
		{
			name: "Regular user returns false",
			user: User{
				ID:       2,
				Username: "user",
				Role:     UserRoleUser,
			},
			expected: false,
		},
		{
			name: "Empty role returns false",
			user: User{
				ID:       3,
				Username: "noRole",
				Role:     "",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.user.IsAdmin()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestUser_IsRegularUser(t *testing.T) {
	tests := []struct {
		name     string
		user     User
		expected bool
	}{
		{
			name: "Regular user returns true",
			user: User{
				ID:       1,
				Username: "user",
				Role:     UserRoleUser,
			},
			expected: true,
		},
		{
			name: "Admin user returns false",
			user: User{
				ID:       2,
				Username: "admin",
				Role:     UserRoleAdmin,
			},
			expected: false,
		},
		{
			name: "Empty role returns false",
			user: User{
				ID:       3,
				Username: "noRole",
				Role:     "",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.user.IsRegularUser()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestUser_ToResponse(t *testing.T) {
	now := time.Now()

	user := User{
		ID:        1,
		Username:  "testuser",
		Email:     "test@example.com",
		Password:  "hashed_password_should_not_be_exposed",
		Role:      UserRoleUser,
		CreatedAt: now,
		UpdatedAt: now,
	}

	response := user.ToResponse()

	// Assert all fields are correctly mapped
	assert.Equal(t, user.ID, response.ID)
	assert.Equal(t, user.Username, response.Username)
	assert.Equal(t, user.Email, response.Email)
	assert.Equal(t, user.Role, response.Role)
	assert.Equal(t, user.CreatedAt, response.CreatedAt)
	assert.Equal(t, user.UpdatedAt, response.UpdatedAt)

	// The password field should not exist in UserResponse
	// We can't directly test this, but the struct definition ensures it
}

func TestUser_TableName(t *testing.T) {
	user := User{}
	tableName := user.TableName()
	assert.Equal(t, "users", tableName)
}

func TestUserRoleConstants(t *testing.T) {
	// Test that role constants have expected values
	assert.Equal(t, "AD", UserRoleAdmin)
	assert.Equal(t, "US", UserRoleUser)
}
