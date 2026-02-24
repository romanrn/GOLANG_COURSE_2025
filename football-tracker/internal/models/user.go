package models

import "time"

// User represents a system user
type User struct {
	ID        int       `json:"id" db:"id"`
	Username  string    `json:"username" db:"username"`
	Email     string    `json:"email" db:"email"`
	Password  string    `json:"-" db:"password"` // Bcrypt hash, never expose in JSON
	Role      string    `json:"role" db:"role"`  // AD=admin, US=user
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName returns the table name for User
func (User) TableName() string {
	return "users"
}

// UserRole constants
const (
	UserRoleAdmin = "AD" // Admin - can edit all data
	UserRoleUser  = "US" // Regular user - can only manage own predictions
)

// IsAdmin checks if user has admin role
func (u *User) IsAdmin() bool {
	return u.Role == UserRoleAdmin
}

// IsRegularUser checks if user has regular user role
func (u *User) IsRegularUser() bool {
	return u.Role == UserRoleUser
}

// UserResponse is used for API responses (without password)
type UserResponse struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ToResponse converts User to UserResponse
func (u *User) ToResponse() UserResponse {
	return UserResponse{
		ID:        u.ID,
		Username:  u.Username,
		Email:     u.Email,
		Role:      u.Role,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}
}
