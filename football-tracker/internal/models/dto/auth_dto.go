package dto

import "time"

// RegisterRequest represents user registration request
type RegisterRequest struct {
	Username string `json:"username" validate:"required,min=3,max=50" example:"john_doe"`
	Email    string `json:"email" validate:"required,email" example:"john.doe@example.com"`
	Password string `json:"password" validate:"required,min=6" example:"SecurePass123!"`
}

// LoginRequest represents user login request
type LoginRequest struct {
	Login    string `json:"login" validate:"required" example:"john.doe@example.com"` // Can be username or email
	Password string `json:"password" validate:"required" example:"SecurePass123!"`
}

// AuthResponse represents authentication response with user data
type AuthResponse struct {
	User  UserDTO `json:"user"`
	Token string  `json:"token,omitempty" example:"session_token_abc123"` // For future JWT implementation
}

// RegisterResponse represents registration response (alias for AuthResponse)
type RegisterResponse = AuthResponse

// LoginResponse represents login response (alias for AuthResponse)
type LoginResponse = AuthResponse

// UserDTO represents user data in responses
type UserDTO struct {
	ID        int       `json:"id" example:"1"`
	Username  string    `json:"username" example:"john_doe"`
	Email     string    `json:"email" example:"john.doe@example.com"`
	Role      string    `json:"role" example:"user"`
	CreatedAt time.Time `json:"created_at" example:"2026-03-01T12:00:00Z"`
	UpdatedAt time.Time `json:"updated_at" example:"2026-03-01T12:00:00Z"`
}

// LogoutResponse represents logout response
type LogoutResponse struct {
	Message string `json:"message" example:"Logout successful"`
}
