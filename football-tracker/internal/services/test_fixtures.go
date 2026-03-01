package services

import (
	"football-tracker/internal/models"
	"football-tracker/internal/testhelpers"
	"time"
)

// Test fixtures for service tests - avoids import cycle by living in services package

// ============================================================================
// User Fixtures
// ============================================================================

func newTestUser() *models.User {
	return &models.User{
		ID:        1,
		Username:  "testuser",
		Email:     "test@example.com",
		Password:  "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy", // "password123"
		Role:      models.UserRoleUser,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func newTestUserWithUsername(username string) *models.User {
	user := newTestUser()
	user.Username = username
	return user
}

func newTestUserWithEmail(email string) *models.User {
	user := newTestUser()
	user.Email = email
	return user
}

func newTestUserWithUsernameAndEmail(username, email string) *models.User {
	user := newTestUser()
	user.Username = username
	user.Email = email
	return user
}

func newTestUserWithPassword(password string) *models.User {
	user := newTestUser()
	user.Password = password
	return user
}

func newTestAdmin() *models.User {
	user := newTestUser()
	user.Role = models.UserRoleAdmin
	return user
}

// ============================================================================
// Session Fixtures
// ============================================================================

func newTestSession() *models.Session {
	return &models.Session{
		ID:        1,
		UserID:    1,
		Token:     "test-token-12345",
		ExpiresAt: time.Now().Add(24 * time.Hour),
		CreatedAt: time.Now(),
	}
}

func newTestSessionWithToken(token string) *models.Session {
	session := newTestSession()
	session.Token = token
	return session
}

func newTestSessionWithUserID(userID int) *models.Session {
	session := newTestSession()
	session.UserID = userID
	return session
}

func newTestSessionWithTokenAndUserID(token string, userID int) *models.Session {
	session := newTestSession()
	session.Token = token
	session.UserID = userID
	return session
}

func newTestExpiredSession() *models.Session {
	return &models.Session{
		ID:        1,
		UserID:    1,
		Token:     "expired-token",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
		CreatedAt: time.Now(),
	}
}

func newTestExpiredSessionWithToken(token string) *models.Session {
	session := newTestExpiredSession()
	session.Token = token
	return session
}

// ============================================================================
// Match Fixtures
// ============================================================================

func newTestMatch() *models.Match {
	return &models.Match{
		ID:             1,
		ChampionshipID: 1,
		HomeTeamID:     1,
		AwayTeamID:     2,
		Stage:          models.MatchStageGroup,
		Status:         models.MatchStatusScheduled,
		MatchDate:      time.Now().Add(24 * time.Hour),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func newTestFinishedMatch(homeScore, awayScore int) *models.Match {
	return &models.Match{
		ID:               1,
		ChampionshipID:   1,
		HomeTeamID:       1,
		AwayTeamID:       2,
		Stage:            models.MatchStageGroup,
		Status:           models.MatchStatusFinished,
		HomeScoreRegular: testhelpers.IntPtr(homeScore),
		AwayScoreRegular: testhelpers.IntPtr(awayScore),
		MatchDate:        time.Now().Add(-24 * time.Hour),
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}
}

func newTestMatchInPast() *models.Match {
	match := newTestMatch()
	match.MatchDate = time.Now().Add(-24 * time.Hour)
	return match
}

// ============================================================================
// Prediction Fixtures
// ============================================================================

func newTestPrediction() *models.Prediction {
	return &models.Prediction{
		ID:               1,
		UserID:           1,
		MatchID:          1,
		HomeScoreRegular: 2,
		AwayScoreRegular: 1,
		Points:           nil,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}
}
