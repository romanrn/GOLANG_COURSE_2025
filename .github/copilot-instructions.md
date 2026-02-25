# Copilot Instructions - Football Tracker

## Project Overview

REST API for tracking football championships, matches, and user predictions. The database schema supports multiple championships (FIFA World Cup, UEFA Euro, etc.) with a comprehensive prediction and rating system for users. Built with Go, Fiber, and PostgreSQL following Clean Architecture principles.

**Key Features:**
- Multi-championship support (not limited to a single tournament)
- User prediction system for match outcomes
- Rating and leaderboard functionality
- Detailed match tracking (group stage, playoffs, scores, penalties)

## Architecture Patterns

### Clean Architecture

The project follows **Clean Architecture** with strict layer separation:

```
cmd/server/           # Entry point, HTTP handlers, middleware
  ├── handlers/       # HTTP request handlers (calls services)
  ├── middlewares/    # Trace, Logger, Error handlers
  ├── config/         # Environment-based configuration
  └── logger/         # Structured logging setup

internal/
  ├── services/       # Business logic (calls repositories)
  ├── out/database/   # Repository implementations (database adapters)
  ├── models/         # Domain models and DTOs
  └── clients/        # External dependencies (PostgreSQL client)
```

**Layer communication flow:** `Handler → Service → Repository → Database`

### Dependency Injection

Always use **constructor-based DI** with interface dependencies:

```go
// Bad - concrete dependency
type service struct {
    repo *postgresRepository
}

// Good - interface dependency
type service struct {
    repo ChampionshipRepository  // interface from internal/out/database/interfaces.go
}

func NewChampionshipService(repo ChampionshipRepository) ChampionshipService {
    return &championshipService{repo: repo}
}
```

See [internal/services/services.go](football-tracker/internal/services/services.go) for DI container pattern.

### Interface Design

- Define repository interfaces in `internal/out/database/interfaces.go`
- Define service interfaces in `internal/services/interfaces.go`
- Implementations follow `<entity>Repository` and `<entity>Service` naming

## Critical Middleware Order

**IMPORTANT:** Middleware registration order in [cmd/server/server.go](football-tracker/cmd/server/server.go) is critical:

```go
app.Use(mdlwrs.Trace.Handle)       // 1. Extract trace_id/span_id from OpenTelemetry
app.Use(mdlwrs.ErrorHandler.Handle) // 2. Centralized error handling
app.Use(mdlwrs.Logger.Handle)      // 3. Request/response logging (includes trace_id)
```

Changing this order breaks trace ID propagation. See [MIDDLEWARE_USAGE_EXAMPLE.md](football-tracker/MIDDLEWARE_USAGE_EXAMPLE.md).

## Logging Standards

Use structured logging with **mandatory attributes**:

```go
logger.GetLogger().Info(ctx, "Querying team by ID",
    slog.String("component", "repository"),  // "handler", "service", or "repository"
    slog.String("method", "GetById"),
    slog.Int("team_id", teamId),
)
```

**Always pass context** to logging methods for automatic trace ID injection. See [docs/LOGGING_IMPLEMENTATION.md](football-tracker/docs/LOGGING_IMPLEMENTATION.md).

## Database Patterns

### Migrations

Single SQL migration file: [migrations/001_init.sql](football-tracker/migrations/001_init.sql)

Apply using Docker commands (see above in Development Workflow section).

Never modify the migration file after applying - create new migration files for schema changes.

### Repository Implementation

Always use parameterized queries and proper error handling:

```go
func (r *teamRepository) GetById(ctx context.Context, teamId int) (models.Team, error) {
    query := `SELECT id, name, country_code FROM teams WHERE id = $1`
    
    var team models.Team
    err := r.db.QueryRowContext(ctx, query, teamId).Scan(&team.ID, &team.Name, &team.CountryCode)
    if err == sql.ErrNoRows {
        return models.Team{}, fmt.Errorf("team with id %d not found", teamId)
    }
    if err != nil {
        return models.Team{}, fmt.Errorf("failed to query team: %w", err)
    }
    
    return team, nil
}
```

Use `QueryRowContext` for single rows, `QueryContext` for multiple rows. Always propagate context.

## Development Workflow

### Building and Running

Use Docker Compose for development:

```bash
docker-compose up -d        # Start all services
docker-compose down         # Stop containers
docker-compose logs -f      # View logs
docker-compose ps           # Check status
docker-compose down -v      # Remove volumes
```

### Database Migrations

```bash
# Apply migrations
docker-compose up -d postgres
sleep 10
docker exec -i football_db psql -U postgres -d footballTracker < migrations/001_init.sql

# Check database tables
docker exec -it football_db psql -U postgres -d footballTracker -c "\dt"

# Access PostgreSQL shell
docker exec -it football_db psql -U postgres -d footballTracker
```

### Docker Development

The project uses multi-stage Dockerfile with non-root user. Production config: [docker-compose.prod.yml](football-tracker/docker-compose.prod.yml)

**Environment variables** (see README for full list):
- `DATABASE_NAME=footballTracker` - must match migration scripts
- `DB_HOST=localhost` for local dev, `postgres` in Docker
- `LOGGER_LEVEL=info` - use `debug` for development

## Technology Stack

- **Web Framework:** Fiber v2 (not Gin/Echo)
- **Database:** PostgreSQL 16+ with `lib/pq` driver
- **Config:** `caarlos0/env/v11` for environment variables
- **Logging:** Standard library `log/slog` (not logrus/zap)
- **Tracing:** OpenTelemetry for distributed tracing

## Key Files Reference

- [CODE_REVIEW.md](football-tracker/CODE_REVIEW.md) - Comprehensive architecture review
- [DATABASE_SCHEMA.md](football-tracker/docs/DATABASE_SCHEMA.md) - Complete DB schema with 14 tables
- [docs/INDEX.md](football-tracker/docs/INDEX.md) - Documentation navigation

## Anti-Patterns to Avoid

❌ Skipping middleware order requirements
❌ Using concrete types instead of interfaces in constructors
❌ Logging without context parameter (breaks trace ID)
❌ Direct database queries in handlers/services (use repositories)
❌ Modifying existing migration files
❌ Missing `component` and `method` in log attributes
