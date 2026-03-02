package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"football-tracker/cmd/server/logger"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"
	repositories "football-tracker/internal/out/database"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUserNotFound       = errors.New("user not found")
	ErrInvalidToken       = errors.New("invalid or expired token")
)

const (
	TokenLength = 32 // 32 bytes = 64 hex characters
)

type authService struct {
	userRepo    repositories.UserRepository
	sessionRepo repositories.SessionRepository
	sessionTTL  time.Duration // TTL from config
}

func NewAuthService(userRepo repositories.UserRepository, sessionRepo repositories.SessionRepository, sessionTTL time.Duration) AuthService {
	return &authService{
		userRepo:    userRepo,
		sessionRepo: sessionRepo,
		sessionTTL:  sessionTTL,
	}
}

// generateSessionToken generates a cryptographically secure random token
func generateSessionToken() (string, error) {
	bytes := make([]byte, TokenLength)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// createSession creates a new session for the user
func (s *authService) createSession(ctx context.Context, userId int) (string, error) {
	token, err := generateSessionToken()
	if err != nil {
		return "", err
	}

	session := &models.Session{
		UserID:    userId,
		Token:     token,
		ExpiresAt: time.Now().Add(s.sessionTTL), // Use config TTL
	}

	err = s.sessionRepo.Create(ctx, session)
	if err != nil {
		return "", err
	}

	return token, nil
}

func (s *authService) Register(ctx context.Context, req dto.RegisterRequest) (*dto.AuthResponse, error) {
	logger.GetLogger().Info(
		ctx,
		"Processing user registration",
		slog.String("component", "service"),
		slog.String("method", "Register"),
		slog.String("username", req.Username),
		slog.String("email", req.Email),
	)

	// Check if username already exists
	existingUser, err := s.userRepo.GetByUsername(ctx, req.Username)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to check existing username",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to check username: %w", err)
	}

	if existingUser != nil {
		logger.GetLogger().Warn(
			ctx,
			"Username already exists",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.String("username", req.Username),
		)
		return nil, ErrUserAlreadyExists
	}

	// Check if email already exists
	existingUser, err = s.userRepo.GetByEmail(ctx, req.Email)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to check existing email",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to check email: %w", err)
	}

	if existingUser != nil {
		logger.GetLogger().Warn(
			ctx,
			"Email already exists",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.String("email", req.Email),
		)
		return nil, ErrUserAlreadyExists
	}

	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to hash password",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// Create user model
	user := &models.User{
		Username: req.Username,
		Email:    req.Email,
		Password: string(hashedPassword),
		Role:     models.UserRoleUser, // Always register as regular user
	}

	// Save user to database
	err = s.userRepo.Create(ctx, user)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to create user",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"User registered successfully",
		slog.String("component", "service"),
		slog.String("method", "Register"),
		slog.Int("user_id", user.ID),
	)

	// Create session token
	token, err := s.createSession(ctx, user.ID)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to create session",
			slog.String("component", "service"),
			slog.String("method", "Register"),
			slog.Int("user_id", user.ID),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	// Prepare response
	response := &dto.AuthResponse{
		User: dto.UserDTO{
			ID:        user.ID,
			Username:  user.Username,
			Email:     user.Email,
			Role:      user.Role,
			CreatedAt: user.CreatedAt,
			UpdatedAt: user.UpdatedAt,
		},
		Token: token,
	}

	return response, nil
}

func (s *authService) Login(ctx context.Context, req dto.LoginRequest) (*dto.AuthResponse, error) {
	logger.GetLogger().Info(
		ctx,
		"Processing user login",
		slog.String("component", "service"),
		slog.String("method", "Login"),
		slog.String("login", req.Login),
	)

	// Try to find user by username or email
	var user *models.User
	var err error

	// Check if login is email (contains @)
	if strings.Contains(req.Login, "@") {
		user, err = s.userRepo.GetByEmail(ctx, req.Login)
	} else {
		user, err = s.userRepo.GetByUsername(ctx, req.Login)
	}

	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to query user",
			slog.String("component", "service"),
			slog.String("method", "Login"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	if user == nil {
		logger.GetLogger().Warn(
			ctx,
			"User not found",
			slog.String("component", "service"),
			slog.String("method", "Login"),
			slog.String("login", req.Login),
		)
		return nil, ErrInvalidCredentials
	}

	// Verify password
	err = bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password))
	if err != nil {
		logger.GetLogger().Warn(
			ctx,
			"Invalid password",
			slog.String("component", "service"),
			slog.String("method", "Login"),
			slog.Int("user_id", user.ID),
		)
		return nil, ErrInvalidCredentials
	}

	logger.GetLogger().Info(
		ctx,
		"User logged in successfully",
		slog.String("component", "service"),
		slog.String("method", "Login"),
		slog.Int("user_id", user.ID),
	)

	// Create session token
	token, err := s.createSession(ctx, user.ID)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to create session",
			slog.String("component", "service"),
			slog.String("method", "Login"),
			slog.Int("user_id", user.ID),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	// Prepare response
	response := &dto.AuthResponse{
		User: dto.UserDTO{
			ID:        user.ID,
			Username:  user.Username,
			Email:     user.Email,
			Role:      user.Role,
			CreatedAt: user.CreatedAt,
			UpdatedAt: user.UpdatedAt,
		},
		Token: token,
	}

	return response, nil
}

func (s *authService) Logout(ctx context.Context, userId int) error {
	logger.GetLogger().Info(
		ctx,
		"Processing user logout",
		slog.String("component", "service"),
		slog.String("method", "Logout"),
		slog.Int("user_id", userId),
	)

	// Verify user exists
	user, err := s.userRepo.GetById(ctx, userId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to query user",
			slog.String("component", "service"),
			slog.String("method", "Logout"),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to query user: %w", err)
	}

	if user == nil {
		logger.GetLogger().Warn(
			ctx,
			"User not found",
			slog.String("component", "service"),
			slog.String("method", "Logout"),
			slog.Int("user_id", userId),
		)
		return ErrUserNotFound
	}

	// Delete all user sessions
	err = s.sessionRepo.DeleteByUserId(ctx, userId)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to delete user sessions",
			slog.String("component", "service"),
			slog.String("method", "Logout"),
			slog.Int("user_id", userId),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to delete sessions: %w", err)
	}

	logger.GetLogger().Info(
		ctx,
		"User logged out successfully",
		slog.String("component", "service"),
		slog.String("method", "Logout"),
		slog.Int("user_id", userId),
	)

	return nil
}

// ValidateSession validates session token and checks TTL
// Returns user if valid, error if invalid or expired
func (s *authService) ValidateSession(ctx context.Context, token string) (*models.User, error) {
	logger.GetLogger().Info(
		ctx,
		"Validating session",
		slog.String("component", "service"),
		slog.String("method", "ValidateSession"),
	)

	// Get session by token
	session, err := s.sessionRepo.GetByToken(ctx, token)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get session",
			slog.String("component", "service"),
			slog.String("method", "ValidateSession"),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	if session == nil {
		logger.GetLogger().Warn(
			ctx,
			"Session not found",
			slog.String("component", "service"),
			slog.String("method", "ValidateSession"),
		)
		return nil, ErrInvalidToken
	}

	// Check if session is expired
	if time.Now().After(session.ExpiresAt) {
		logger.GetLogger().Warn(
			ctx,
			"Session expired",
			slog.String("component", "service"),
			slog.String("method", "ValidateSession"),
			slog.Int("session_id", session.ID),
			slog.Time("expired_at", session.ExpiresAt),
		)

		// Delete expired session
		_ = s.sessionRepo.DeleteByToken(ctx, token)

		return nil, ErrInvalidToken
	}

	// Get user data
	user, err := s.userRepo.GetById(ctx, session.UserID)
	if err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to get user",
			slog.String("component", "service"),
			slog.String("method", "ValidateSession"),
			slog.Int("user_id", session.UserID),
			slog.String("error", err.Error()),
		)
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	if user == nil {
		logger.GetLogger().Warn(
			ctx,
			"User not found for session",
			slog.String("component", "service"),
			slog.String("method", "ValidateSession"),
			slog.Int("user_id", session.UserID),
		)
		return nil, ErrUserNotFound
	}

	logger.GetLogger().Info(
		ctx,
		"Session validated successfully",
		slog.String("component", "service"),
		slog.String("method", "ValidateSession"),
		slog.Int("user_id", user.ID),
		slog.String("username", user.Username),
	)

	return user, nil
}
