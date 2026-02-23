package errorHandler

import (
	"errors"

	"github.com/gofiber/fiber/v2"
)

type Middleware struct{}

func NewMiddleware() *Middleware {
	return &Middleware{}
}

func (m *Middleware) Handle(ctx *fiber.Ctx) error {
	err := ctx.Next()

	if err == nil {
		return nil
	}

	var fe *fiber.Error
	if errors.As(err, &fe) {
		return ctx.Status(fe.Code).JSON(fiber.Map{
			"error": fe.Message,
		})
	}

	return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
		"error": err.Error(),
	})
}
