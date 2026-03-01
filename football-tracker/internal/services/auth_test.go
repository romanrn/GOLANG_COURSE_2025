//go:build unit
// +build unit

package services

import (
	"errors"
	"football-tracker/internal/models"
	"football-tracker/internal/models/dto"
	"football-tracker/internal/out/database/mocks"
	"football-tracker/internal/testhelpers"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/crypto/bcrypt"
)

func TestAuthService_Register(t *testing.T) {
	tests := []struct {
		name              string
		request           dto.RegisterRequest
		mockUserRepo      func(*mocks.MockUserRepository)
		mockSessionRepo   func(*mocks.MockSessionRepository)
		expectedError     bool
		expectedErrorType error
		errorContains     string
	}{
		{
			name: "Success - register new user",
			request: dto.RegisterRequest{
				Username: "testuser",
				Email:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "testuser").Return(nil, nil)
				m.On("GetByEmail", mock.Anything, "test@example.com").Return(nil, nil)
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.User")).Return(nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.Session")).Return(nil)
			},
			expectedError: false,
		},
		{
			name: "Error - username already exists",
			request: dto.RegisterRequest{
				Username: "existinguser",
				Email:    "new@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				existingUser := newTestUserWithUsernameAndEmail("existinguser", "existing@example.com")
				m.On("GetByUsername", mock.Anything, "existinguser").Return(existingUser, nil)
			},
			mockSessionRepo:   func(m *mocks.MockSessionRepository) {},
			expectedError:     true,
			expectedErrorType: ErrUserAlreadyExists,
		},
		{
			name: "Error - email already exists",
			request: dto.RegisterRequest{
				Username: "newuser",
				Email:    "existing@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "newuser").Return(nil, nil)
				existingUser := newTestUserWithUsernameAndEmail("existinguser", "existing@example.com")
				m.On("GetByEmail", mock.Anything, "existing@example.com").Return(existingUser, nil)
			},
			mockSessionRepo:   func(m *mocks.MockSessionRepository) {},
			expectedError:     true,
			expectedErrorType: ErrUserAlreadyExists,
		},
		{
			name: "Error - repository create fails",
			request: dto.RegisterRequest{
				Username: "testuser",
				Email:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "testuser").Return(nil, nil)
				m.On("GetByEmail", mock.Anything, "test@example.com").Return(nil, nil)
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.User")).Return(errors.New("database error"))
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
		},
		{
			name: "Error - session creation fails",
			request: dto.RegisterRequest{
				Username: "testuser",
				Email:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "testuser").Return(nil, nil)
				m.On("GetByEmail", mock.Anything, "test@example.com").Return(nil, nil)
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.User")).Return(nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.Session")).Return(errors.New("session error"))
			},
			expectedError: true,
		},
		{
			name: "Error - repository GetByUsername fails",
			request: dto.RegisterRequest{
				Username: "testuser",
				Email:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "testuser").Return(nil, errors.New("username lookup failed"))
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
			errorContains:   "failed to check username",
		},
		{
			name: "Error - repository GetByEmail fails",
			request: dto.RegisterRequest{
				Username: "testuser",
				Email:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "testuser").Return(nil, nil)
				m.On("GetByEmail", mock.Anything, "test@example.com").Return(nil, errors.New("email lookup failed"))
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
			errorContains:   "failed to check email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockUserRepo := new(mocks.MockUserRepository)
			mockSessionRepo := new(mocks.MockSessionRepository)
			tt.mockUserRepo(mockUserRepo)
			tt.mockSessionRepo(mockSessionRepo)

			service := NewAuthService(mockUserRepo, mockSessionRepo, 24*time.Hour)
			ctx := testhelpers.DefaultTestContext(t)

			// Act
			response, err := service.Register(ctx, tt.request)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
				if tt.expectedErrorType != nil {
					assert.ErrorIs(t, err, tt.expectedErrorType)
				}
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.Equal(t, tt.request.Username, response.User.Username)
				assert.Equal(t, tt.request.Email, response.User.Email)
				assert.NotEmpty(t, response.Token)
				assert.Equal(t, models.UserRoleUser, response.User.Role)
			}
			mockUserRepo.AssertExpectations(t)
			mockSessionRepo.AssertExpectations(t)
		})
	}
}

func TestAuthService_Login(t *testing.T) {
	// Create a hashed password for testing
	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte("password123"), bcrypt.DefaultCost)

	tests := []struct {
		name              string
		request           dto.LoginRequest
		mockUserRepo      func(*mocks.MockUserRepository)
		mockSessionRepo   func(*mocks.MockSessionRepository)
		expectedError     bool
		expectedErrorType error
		errorContains     string
	}{
		{
			name: "Success - login with username",
			request: dto.LoginRequest{
				Login:    "testuser",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUserWithPassword(string(hashedPassword))
				m.On("GetByUsername", mock.Anything, "testuser").Return(user, nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.Session")).Return(nil)
			},
			expectedError: false,
		},
		{
			name: "Success - login with email",
			request: dto.LoginRequest{
				Login:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUserWithPassword(string(hashedPassword))
				m.On("GetByEmail", mock.Anything, "test@example.com").Return(user, nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.Session")).Return(nil)
			},
			expectedError: false,
		},
		{
			name: "Error - user not found",
			request: dto.LoginRequest{
				Login:    "nonexistent",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "nonexistent").Return(nil, nil)
			},
			mockSessionRepo:   func(m *mocks.MockSessionRepository) {},
			expectedError:     true,
			expectedErrorType: ErrInvalidCredentials,
		},
		{
			name: "Error - wrong password",
			request: dto.LoginRequest{
				Login:    "testuser",
				Password: "wrongpassword",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUserWithPassword(string(hashedPassword))
				m.On("GetByUsername", mock.Anything, "testuser").Return(user, nil)
			},
			mockSessionRepo:   func(m *mocks.MockSessionRepository) {},
			expectedError:     true,
			expectedErrorType: ErrInvalidCredentials,
		},
		{
			name: "Error - session creation fails",
			request: dto.LoginRequest{
				Login:    "testuser",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUserWithPassword(string(hashedPassword))
				m.On("GetByUsername", mock.Anything, "testuser").Return(user, nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("Create", mock.Anything, mock.AnythingOfType("*models.Session")).Return(errors.New("session error"))
			},
			expectedError: true,
		},
		{
			name: "Error - repository error (username login)",
			request: dto.LoginRequest{
				Login:    "testuser",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByUsername", mock.Anything, "testuser").Return(nil, errors.New("query failed"))
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
			errorContains:   "failed to query user",
		},
		{
			name: "Error - repository error (email login)",
			request: dto.LoginRequest{
				Login:    "test@example.com",
				Password: "password123",
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetByEmail", mock.Anything, "test@example.com").Return(nil, errors.New("query failed"))
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
			errorContains:   "failed to query user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockUserRepo := new(mocks.MockUserRepository)
			mockSessionRepo := new(mocks.MockSessionRepository)
			tt.mockUserRepo(mockUserRepo)
			tt.mockSessionRepo(mockSessionRepo)

			service := NewAuthService(mockUserRepo, mockSessionRepo, 24*time.Hour)
			ctx := testhelpers.DefaultTestContext(t)

			// Act
			response, err := service.Login(ctx, tt.request)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
				if tt.expectedErrorType != nil {
					assert.ErrorIs(t, err, tt.expectedErrorType)
				}
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response)
				assert.NotEmpty(t, response.Token)
			}
			mockUserRepo.AssertExpectations(t)
			mockSessionRepo.AssertExpectations(t)
		})
	}
}

func TestAuthService_Logout(t *testing.T) {
	tests := []struct {
		name            string
		userID          int
		mockUserRepo    func(*mocks.MockUserRepository)
		mockSessionRepo func(*mocks.MockSessionRepository)
		expectedError   bool
		errorType       error
	}{
		{
			name:   "Success - logout user",
			userID: 1,
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUser()
				m.On("GetById", mock.Anything, 1).Return(user, nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("DeleteByUserId", mock.Anything, 1).Return(nil)
			},
			expectedError: false,
		},
		{
			name:   "Error - repository error",
			userID: 1,
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUser()
				m.On("GetById", mock.Anything, 1).Return(user, nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("DeleteByUserId", mock.Anything, 1).Return(errors.New("database error"))
			},
			expectedError: true,
		},
		{
			name:   "Error - user not found",
			userID: 1,
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetById", mock.Anything, 1).Return(nil, nil)
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
			errorType:       ErrUserNotFound,
		},
		{
			name:   "Error - repository GetById fails",
			userID: 1,
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetById", mock.Anything, 1).Return(nil, errors.New("query failed"))
			},
			mockSessionRepo: func(m *mocks.MockSessionRepository) {},
			expectedError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockUserRepo := new(mocks.MockUserRepository)
			mockSessionRepo := new(mocks.MockSessionRepository)
			tt.mockUserRepo(mockUserRepo)
			tt.mockSessionRepo(mockSessionRepo)

			service := NewAuthService(mockUserRepo, mockSessionRepo, 24*time.Hour)
			ctx := testhelpers.DefaultTestContext(t)

			// Act
			err := service.Logout(ctx, tt.userID)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorType != nil {
					assert.ErrorIs(t, err, tt.errorType)
				}
			} else {
				assert.NoError(t, err)
			}
			mockUserRepo.AssertExpectations(t)
			mockSessionRepo.AssertExpectations(t)
		})
	}
}

func TestAuthService_ValidateSession(t *testing.T) {
	tests := []struct {
		name              string
		token             string
		mockSessionRepo   func(*mocks.MockSessionRepository)
		mockUserRepo      func(*mocks.MockUserRepository)
		expectedError     bool
		expectedErrorType error
		errorContains     string
	}{
		{
			name:  "Success - valid session",
			token: "valid_token_12345",
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				session := newTestSessionWithTokenAndUserID("valid_token_12345", 1)
				m.On("GetByToken", mock.Anything, "valid_token_12345").Return(session, nil)
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				user := newTestUser()
				m.On("GetById", mock.Anything, 1).Return(user, nil)
			},
			expectedError: false,
		},
		{
			name:  "Error - session not found",
			token: "invalid_token",
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("GetByToken", mock.Anything, "invalid_token").Return(nil, nil)
			},
			mockUserRepo:      func(m *mocks.MockUserRepository) {},
			expectedError:     true,
			expectedErrorType: ErrInvalidToken,
		},
		{
			name:  "Error - expired session",
			token: "expired_token",
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				session := newTestExpiredSessionWithToken("expired_token")
				m.On("GetByToken", mock.Anything, "expired_token").Return(session, nil)
				m.On("DeleteByToken", mock.Anything, "expired_token").Return(nil) // Delete expired session
			},
			mockUserRepo:      func(m *mocks.MockUserRepository) {},
			expectedError:     true,
			expectedErrorType: ErrInvalidToken,
		},
		{
			name:  "Error - user not found",
			token: "valid_token_12345",
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				session := newTestSessionWithTokenAndUserID("valid_token_12345", 999)
				m.On("GetByToken", mock.Anything, "valid_token_12345").Return(session, nil)
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetById", mock.Anything, 999).Return(nil, nil)
			},
			expectedError:     true,
			expectedErrorType: ErrUserNotFound,
		},
		{
			name:  "Error - session repository error",
			token: "valid_token_12345",
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				m.On("GetByToken", mock.Anything, "valid_token_12345").Return(nil, errors.New("session query failed"))
			},
			mockUserRepo:  func(m *mocks.MockUserRepository) {},
			expectedError: true,
			errorContains: "failed to get session",
		},
		{
			name:  "Error - user repository error",
			token: "valid_token_12345",
			mockSessionRepo: func(m *mocks.MockSessionRepository) {
				session := newTestSessionWithTokenAndUserID("valid_token_12345", 1)
				m.On("GetByToken", mock.Anything, "valid_token_12345").Return(session, nil)
			},
			mockUserRepo: func(m *mocks.MockUserRepository) {
				m.On("GetById", mock.Anything, 1).Return(nil, errors.New("user query failed"))
			},
			expectedError: true,
			errorContains: "failed to get user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockUserRepo := new(mocks.MockUserRepository)
			mockSessionRepo := new(mocks.MockSessionRepository)
			tt.mockSessionRepo(mockSessionRepo)
			tt.mockUserRepo(mockUserRepo)

			service := NewAuthService(mockUserRepo, mockSessionRepo, 24*time.Hour)
			ctx := testhelpers.DefaultTestContext(t)

			// Act
			user, err := service.ValidateSession(ctx, tt.token)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
				if tt.expectedErrorType != nil {
					assert.ErrorIs(t, err, tt.expectedErrorType)
				}
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, user)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, user)
			}
			mockSessionRepo.AssertExpectations(t)
			mockUserRepo.AssertExpectations(t)
		})
	}
}
