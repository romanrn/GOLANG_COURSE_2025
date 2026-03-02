package auth

import (
	"context"
	"football-tracker/cmd/server/logger"
	"football-tracker/cmd/server/middlewares/otel"
	"football-tracker/internal/models"
	"football-tracker/internal/services"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

const (
	SessionCookieName = "session_token"
	UserContextKey    = "user"
	UserIDContextKey  = "user_id"
)

type Middleware struct {
	authService services.AuthService
}

func NewAuthMiddleware(authService services.AuthService) *Middleware {
	return &Middleware{
		authService: authService,
	}
}

// Handle validates session token from cookie and adds user to context
func (m *Middleware) Handle(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger.GetLogger().Debug(
		ctx,
		"Auth middleware: validating session",
		slog.String("component", "middleware"),
		slog.String("method", "Handle"),
		slog.String("path", c.Path()),
	)

	// Extract session token from cookie
	token := c.Cookies(SessionCookieName)
	if token == "" {
		logger.GetLogger().Warn(
			ctx,
			"Auth middleware: no session token in cookie",
			slog.String("component", "middleware"),
			slog.String("method", "Handle"),
			slog.String("path", c.Path()),
		)
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "Authentication required",
		})
	}

	// Validate session through service layer
	user, err := m.authService.ValidateSession(ctx, token)
	if err != nil {
		logger.GetLogger().Warn(
			ctx,
			"Auth middleware: invalid session",
			slog.String("component", "middleware"),
			slog.String("method", "Handle"),
			slog.String("error", err.Error()),
		)
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "Invalid or expired session",
		})
	}

	logger.GetLogger().Debug(
		ctx,
		"Auth middleware: session valid",
		slog.String("component", "middleware"),
		slog.String("method", "Handle"),
		slog.Int("user_id", user.ID),
		slog.String("username", user.Username),
		slog.String("role", user.Role),
	)

	// Add user data to context for handlers
	ctx = context.WithValue(ctx, UserContextKey, user)
	ctx = context.WithValue(ctx, UserIDContextKey, user.ID)
	// Also add to otel.UserIDKey for automatic logging enrichment
	ctx = context.WithValue(ctx, otel.UserIDKey, user.ID)
	c.SetUserContext(ctx)

	return c.Next()
}

// OptionalHandle validates session token if present, but doesn't require authentication
// This allows public endpoints to log user_id for authenticated users
func (m *Middleware) OptionalHandle(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger.GetLogger().Debug(
		ctx,
		"Optional auth middleware: checking for session",
		slog.String("component", "middleware"),
		slog.String("method", "OptionalHandle"),
		slog.String("path", c.Path()),
	)

	// Extract session token from cookie
	token := c.Cookies(SessionCookieName)
	if token == "" {
		logger.GetLogger().Debug(
			ctx,
			"Optional auth middleware: no session token, continuing as anonymous",
			slog.String("component", "middleware"),
			slog.String("method", "OptionalHandle"),
			slog.String("path", c.Path()),
		)
		// No token - continue without user context
		return c.Next()
	}

	// Validate session through service layer
	user, err := m.authService.ValidateSession(ctx, token)
	if err != nil {
		logger.GetLogger().Debug(
			ctx,
			"Optional auth middleware: invalid session, continuing as anonymous",
			slog.String("component", "middleware"),
			slog.String("method", "OptionalHandle"),
			slog.String("error", err.Error()),
		)
		// Invalid token - continue without user context (don't return error)
		return c.Next()
	}

	logger.GetLogger().Debug(
		ctx,
		"Optional auth middleware: session valid, adding user to context",
		slog.String("component", "middleware"),
		slog.String("method", "OptionalHandle"),
		slog.Int("user_id", user.ID),
		slog.String("username", user.Username),
		slog.String("role", user.Role),
	)

	// Add user data to context for handlers
	ctx = context.WithValue(ctx, UserContextKey, user)
	ctx = context.WithValue(ctx, UserIDContextKey, user.ID)
	// Also add to otel.UserIDKey for automatic logging enrichment
	ctx = context.WithValue(ctx, otel.UserIDKey, user.ID)
	c.SetUserContext(ctx)

	return c.Next()
}

// RequireRole creates a middleware that checks user role
func (m *Middleware) RequireRole(requiredRole string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := c.UserContext()

		// Extract user from context (set by Handle middleware)
		userValue := ctx.Value(UserContextKey)
		if userValue == nil {
			logger.GetLogger().Warn(
				ctx,
				"Role middleware: user not found in context",
				slog.String("component", "middleware"),
				slog.String("method", "RequireRole"),
				slog.String("required_role", requiredRole),
			)
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Authentication required",
			})
		}

		user := userValue.(*models.User)

		// Check if user has required role
		if user.Role != requiredRole {
			logger.GetLogger().Warn(
				ctx,
				"Role middleware: insufficient permissions",
				slog.String("component", "middleware"),
				slog.String("method", "RequireRole"),
				slog.Int("user_id", user.ID),
				slog.String("user_role", user.Role),
				slog.String("required_role", requiredRole),
			)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "Insufficient permissions",
			})
		}

		logger.GetLogger().Debug(
			ctx,
			"Role middleware: access granted",
			slog.String("component", "middleware"),
			slog.String("method", "RequireRole"),
			slog.Int("user_id", user.ID),
			slog.String("role", user.Role),
		)

		return c.Next()
	}
}
