package auth

import (
	"context"
	"errors"
	"football-tracker/cmd/server/config"
	"football-tracker/cmd/server/logger"
	"football-tracker/cmd/server/middlewares/otel"
	"football-tracker/internal/models/dto"
	"football-tracker/internal/services"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	authService services.AuthService
	config      *config.ServerConfig
}

func NewHandler(authService services.AuthService, cfg *config.ServerConfig) *Handler {
	return &Handler{
		authService: authService,
		config:      cfg,
	}
}

// Register handles user registration
// POST /api/v1/auth/register
func (h *Handler) Register(c *fiber.Ctx) error {
	ctx := c.Context()

	logger.GetLogger().Info(
		ctx,
		"Register endpoint called",
		slog.String("component", "handler"),
		slog.String("method", "Register"),
	)

	var req dto.RegisterRequest
	if err := c.BodyParser(&req); err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to parse request body",
			slog.String("component", "handler"),
			slog.String("method", "Register"),
			slog.String("error", err.Error()),
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Basic validation
	if req.Username == "" || req.Email == "" || req.Password == "" {
		logger.GetLogger().Warn(
			ctx,
			"Missing required fields",
			slog.String("component", "handler"),
			slog.String("method", "Register"),
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Username, email, and password are required",
		})
	}

	if len(req.Password) < 6 {
		logger.GetLogger().Warn(
			ctx,
			"Password too short",
			slog.String("component", "handler"),
			slog.String("method", "Register"),
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Password must be at least 6 characters",
		})
	}

	response, err := h.authService.Register(ctx, req)
	if err != nil {
		if errors.Is(err, services.ErrUserAlreadyExists) {
			logger.GetLogger().Warn(
				ctx,
				"User already exists",
				slog.String("component", "handler"),
				slog.String("method", "Register"),
			)
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "User with this username or email already exists",
			})
		}

		logger.GetLogger().Error(
			ctx,
			"Failed to register user",
			slog.String("component", "handler"),
			slog.String("method", "Register"),
			slog.String("error", err.Error()),
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to register user",
		})
	}

	logger.GetLogger().Info(
		ctx,
		"User registered successfully",
		slog.String("component", "handler"),
		slog.String("method", "Register"),
		slog.Int("user_id", response.User.ID),
	)

	// Add user_id to context for subsequent logs in this request
	userCtx := context.WithValue(ctx, otel.UserIDKey, response.User.ID)
	c.SetUserContext(userCtx)

	// Set session cookie
	c.Cookie(&fiber.Cookie{
		Name:     "session_token",
		Value:    response.Token,
		HTTPOnly: true,
		Secure:   h.config.SessionCookieSecure, // From config
		SameSite: "Lax",
		Domain:   h.config.SessionCookieDomain,            // From config
		MaxAge:   int(h.config.SessionTokenTTL.Seconds()), // From config (convert to seconds)
	})

	return c.Status(fiber.StatusCreated).JSON(response)
}

// Login handles user login
// POST /api/v1/auth/login
func (h *Handler) Login(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger.GetLogger().Info(
		ctx,
		"Login endpoint called",
		slog.String("component", "handler"),
		slog.String("method", "Login"),
	)

	var req dto.LoginRequest
	if err := c.BodyParser(&req); err != nil {
		logger.GetLogger().Error(
			ctx,
			"Failed to parse request body",
			slog.String("component", "handler"),
			slog.String("method", "Login"),
			slog.String("error", err.Error()),
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Basic validation
	if req.Login == "" || req.Password == "" {
		logger.GetLogger().Warn(
			ctx,
			"Missing required fields",
			slog.String("component", "handler"),
			slog.String("method", "Login"),
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Login and password are required",
		})
	}

	response, err := h.authService.Login(ctx, req)
	if err != nil {
		if errors.Is(err, services.ErrInvalidCredentials) {
			logger.GetLogger().Warn(
				ctx,
				"Invalid credentials",
				slog.String("component", "handler"),
				slog.String("method", "Login"),
			)
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid credentials",
			})
		}

		logger.GetLogger().Error(
			ctx,
			"Failed to login user",
			slog.String("component", "handler"),
			slog.String("method", "Login"),
			slog.String("error", err.Error()),
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to login user",
		})
	}

	logger.GetLogger().Info(
		ctx,
		"User logged in successfully",
		slog.String("component", "handler"),
		slog.String("method", "Login"),
		slog.Int("user_id", response.User.ID),
	)

	// Add user_id to context for subsequent logs in this request
	userCtx := context.WithValue(ctx, otel.UserIDKey, response.User.ID)
	c.SetUserContext(userCtx)

	// Set session cookie
	c.Cookie(&fiber.Cookie{
		Name:     "session_token",
		Value:    response.Token,
		HTTPOnly: true,
		Secure:   h.config.SessionCookieSecure, // From config
		SameSite: "Lax",
		Domain:   h.config.SessionCookieDomain,            // From config
		MaxAge:   int(h.config.SessionTokenTTL.Seconds()), // From config
	})

	return c.Status(fiber.StatusOK).JSON(response)
}

// Logout handles user logout
// POST /api/v1/auth/logout
// Requires authentication (Auth middleware must be applied)
func (h *Handler) Logout(c *fiber.Ctx) error {
	ctx := c.UserContext()

	logger.GetLogger().Info(
		ctx,
		"Logout endpoint called",
		slog.String("component", "handler"),
		slog.String("method", "Logout"),
	)

	// Extract user_id from context (set by Auth middleware)
	userIdValue := ctx.Value("user_id")
	if userIdValue == nil {
		logger.GetLogger().Error(
			ctx,
			"User ID not found in context",
			slog.String("component", "handler"),
			slog.String("method", "Logout"),
		)
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "Authentication required",
		})
	}

	userId, ok := userIdValue.(int)
	if !ok {
		logger.GetLogger().Error(
			ctx,
			"Invalid user ID type in context",
			slog.String("component", "handler"),
			slog.String("method", "Logout"),
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Internal server error",
		})
	}

	err := h.authService.Logout(ctx, userId)
	if err != nil {
		if errors.Is(err, services.ErrUserNotFound) {
			logger.GetLogger().Warn(
				ctx,
				"User not found",
				slog.String("component", "handler"),
				slog.String("method", "Logout"),
			)
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "User not found",
			})
		}

		logger.GetLogger().Error(
			ctx,
			"Failed to logout user",
			slog.String("component", "handler"),
			slog.String("method", "Logout"),
			slog.String("error", err.Error()),
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to logout user",
		})
	}

	logger.GetLogger().Info(
		ctx,
		"User logged out successfully",
		slog.String("component", "handler"),
		slog.String("method", "Logout"),
		slog.Int("user_id", userId),
	)

	// Clear session cookie
	c.Cookie(&fiber.Cookie{
		Name:     "session_token",
		Value:    "",
		HTTPOnly: true,
		Secure:   h.config.SessionCookieSecure,
		SameSite: "Lax",
		MaxAge:   -1, // Delete cookie
	})

	return c.Status(fiber.StatusOK).JSON(dto.LogoutResponse{
		Message: "Logged out successfully",
	})
}
