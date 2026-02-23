package health

import (
	"football-tracker/cmd/server/config"

	"github.com/gofiber/fiber/v2"
)

type Response struct {
	Status string `json:"status"`
	Env    string `json:"env"`
}

type Handler struct {
	cfg *config.ServerConfig
}

func NewHandler(cfg *config.ServerConfig) *Handler {
	return &Handler{
		cfg: cfg,
	}
}

func (h *Handler) Check(c *fiber.Ctx) error {
	return c.JSON(&Response{
		Status: "ok",
		Env:    h.cfg.Enviroment,
	})
}
