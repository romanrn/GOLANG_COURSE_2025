package dto

import "time"

// RegisterRequest represents user registration request
type RegisterRequest struct {
	Username string `json:"username" validate:"required,min=3,max=50"`
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required,min=6"`
}

// LoginRequest represents user login request
type LoginRequest struct {
	Login    string `json:"login" validate:"required"` // Can be username or email
	Password string `json:"password" validate:"required"`
}

// AuthResponse represents authentication response with user data
type AuthResponse struct {
	User  UserDTO `json:"user"`
	Token string  `json:"token,omitempty"` // For future JWT implementation
}

// UserDTO represents user data in responses
type UserDTO struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// LogoutResponse represents logout response
type LogoutResponse struct {
	Message string `json:"message"`
}
