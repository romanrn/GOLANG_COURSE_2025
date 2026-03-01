package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type ServerConfig struct {
	Host            string        `env:"SERVER_HOST" envDefault:"localhost"`
	Port            string        `env:"SERVER_PORT" envDefault:"8080"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT" envDefault:"10s"`
	WriteTimeout    time.Duration `env:"WRITE_TIMEOUT" envDefault:"10s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"`
	AppName         string        `env:"APP_NAME" envDefault:"Football Tracker v1.0"`
	DbName          string        `env:"DATABASE_NAME" envDefault:"footballTracker"`
	DbHost          string        `env:"DB_HOST" envDefault:"localhost"`
	DbPort          string        `env:"DB_PORT" envDefault:"5432"`
	DbUri           string        `env:"DB_URI" envDefault:""`
	DbUserName      string        `env:"DATABASE_USERNAME" envDefault:"postgres"`
	DbPsw           string        `env:"DATABASE_PASSWORD" envDefault:"postgres"`
	MaxPoolSize     uint64        `env:"MAX_POOL_SIZE" envDefault:"100"`
	MinPoolSize     uint64        `env:"MIN_POOL_SIZE" envDefault:"10"`
	MaxConnIdleTime time.Duration `env:"MAX_CONN_IDLE_TIME" envDefault:"30s"`
	MaxConnLifetime time.Duration `env:"MAX_CONN_LIFE_TIME" envDefault:"30m"`
	LoggerLevel     string        `env:"LOGGER_LEVEL" envDefault:"info"`
	Enviroment      string        `env:"APP_ENV" envDefault:"development"`

	// Session Cookie Configuration
	SessionTokenTTL     time.Duration `env:"SESSION_TOKEN_TTL" envDefault:"24h"`
	SessionCookieSecure bool          `env:"SESSION_COOKIE_SECURE" envDefault:"false"`
	SessionCookieDomain string        `env:"SESSION_COOKIE_DOMAIN" envDefault:""`

	// OpenTelemetry Configuration
	OtelEnabled     bool   `env:"OTEL_ENABLED" envDefault:"true"`
	OtelEndpoint    string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"tempo:4317"`
	OtelServiceName string `env:"OTEL_SERVICE_NAME" envDefault:"football-tracker"`
}

func NewConfigFromEnv() (*ServerConfig, error) {
	cfg := &ServerConfig{}
	err := env.Parse(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	return cfg, nil
}
