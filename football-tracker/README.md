# Football Tracker API

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-Proprietary-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)](https://www.docker.com/)

> **Production-ready REST API** for tracking football championships, matches, and user predictions with comprehensive observability stack.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [API Documentation](#api-documentation)
- [Observability](#observability)
- [Testing](#testing)
- [Security](#security)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

**Football Tracker** is a backend service designed for tracking football championships (FIFA World Cup, UEFA Euro, etc.) with a sophisticated user prediction system. The platform enables users to predict match outcomes and compete on leaderboards through an accurate rating system.

### Key Capabilities

- **Multi-Championship Support**: Track multiple tournaments simultaneously (not limited to a single championship)
- **User Prediction Engine**: Advanced scoring algorithm with support for regular time, extra time, and penalty predictions
- **Real-time Leaderboards**: Championship-specific and global user rankings
- **Comprehensive Match Tracking**: Group stages, playoffs, scores, penalties, and detailed match metadata
- **Production-Grade Observability**: Full integration with OpenTelemetry, Prometheus, Grafana, Loki, and Tempo

### Business Value

- **Sports Platforms**: Integrate prediction leagues for fan engagement
- **Media Companies**: Enhance viewer participation during tournaments
- **Gaming Applications**: Power prediction-based competitions
- **Educational**: Demonstration of enterprise-grade Go microservice architecture

---

## ✨ Features

### Core Functionality

#### 1. **Championship Management**
- Multi-tournament support (World Cup, Euro, Copa America, etc.)
- Host country and city tracking
- Group stage and playoff bracket management
- Team assignments and metadata

#### 2. **Match System**
- Detailed match tracking:
  - Regular time scores (90 minutes)
  - Extra time scores (if applicable)
  - Penalty shootout results
- Match status lifecycle: Scheduled → Live → Finished → Cancelled/Postponed
- Stage tracking: Group Stage, Round of 16, Quarter-Finals, Semi-Finals, Finals, Third Place
- City/stadium associations with timezone support

#### 3. **User Prediction Engine**
- **Flexible Prediction Types**:
  - Regular time score (required for all matches)
  - Total score with extra time (optional for playoffs)
  - Penalty shootout winner (optional for playoffs)
  
- **Points System**:
  - Exact score: **3 points**
  - Correct outcome (win/draw/loss): **1 point**
  - Wrong prediction: **0 points**
  - Bonus points for extra time and penalty accuracy

#### 4. **Rating & Leaderboard System**
- **Championship-specific ratings**:
  - Total points, accuracy percentage
  - Correct predictions count
  - Average points per prediction
  - Dynamic ranking updates
  
- **Global user ratings**:
  - Aggregate stats across all championships
  - Historical performance tracking
  - Global leaderboard

#### 5. **Authentication & Authorization**
- Cookie-based session management with configurable TTL
- Role-based access control (Admin/User)
- Secure password hashing with bcrypt (cost 10)
- Session cleanup background job

### Technical Features

- **Clean Architecture** with strict layer separation
- **Dependency Injection** with interface-based design
- **Distributed Tracing** with W3C Trace Context propagation
- **Structured Logging** with automatic trace enrichment
- **Graceful Shutdown** with configurable timeouts
- **Health Checks**: `/healthCheck` (liveness) and `/ready` (readiness)
- **OpenAPI/Swagger** documentation
- **Rate Limiting** and security headers via Nginx
- **HTTPS Support** with Let's Encrypt integration

---

## 🛠 Technology Stack

### Backend

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Runtime** | Go | 1.25+ | Core programming language |
| **Web Framework** | Fiber | v2.52+ | High-performance HTTP server |
| **Database** | PostgreSQL | 16-alpine | Relational data storage |
| **DB Driver** | lib/pq | v1.11+ | PostgreSQL driver |
| **Config Management** | caarlos0/env | v11 | Environment-based configuration |
| **Password Hashing** | golang.org/x/crypto/bcrypt | - | Secure password storage |

### Observability & Monitoring

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Tracing** | OpenTelemetry | v1.40 | Distributed tracing with W3C propagation |
| **Trace Storage** | Grafana Tempo | latest | Trace backend and query service |
| **Metrics** | Prometheus | latest | Metrics collection and storage |
| **OTLP Collector** | OTel Collector Contrib | latest | Metrics & traces processing pipeline |
| **Log Aggregation** | Grafana Loki | latest | Centralized log storage |
| **Log Collection** | Promtail | latest | Docker container log collection |
| **Visualization** | Grafana | latest | Unified observability dashboards |

### Infrastructure

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Containerization** | Docker | Multi-stage builds, non-root user |
| **Orchestration** | Docker Compose | Local development and production |
| **Reverse Proxy** | Nginx | HTTPS termination, rate limiting, security headers |
| **API Documentation** | Swagger/OpenAPI | Interactive API documentation |

### Development Tools

- **Testing**: `testify` for unit and integration tests
- **Code Coverage**: Built-in Go coverage tools with 80%+ threshold
- **Linting**: `golangci-lint` (optional)
- **Migrations**: SQL scripts with idempotent inserts
- **Build System**: Makefile with comprehensive targets

---

## 🏗 Architecture

### Clean Architecture Pattern

The project follows **Clean Architecture** principles with strict layer separation:

```
┌─────────────────────────────────────────────────────────────┐
│                     Presentation Layer                       │
│  cmd/server/handlers/  - HTTP request handlers (Fiber)     │
│  cmd/server/middlewares/ - OTEL, Auth, Logger, Error       │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      Business Logic Layer                    │
│  internal/services/  - Domain logic and validation          │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    Data Access Layer                         │
│  internal/out/database/  - Repository implementations       │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      Infrastructure                          │
│  internal/clients/  - PostgreSQL client                     │
│  internal/models/   - Domain models and DTOs                │
│  internal/telemetry/ - OpenTelemetry setup                  │
│  internal/jobs/     - Background jobs (session cleanup)     │
└─────────────────────────────────────────────────────────────┘
```

### Communication Flow

```
HTTP Request → Middleware Chain → Handler → Service → Repository → Database
                  ↓                   ↓         ↓          ↓
              [OTEL Trace]      [Validation] [Logic]  [SQL Query]
              [Logger]          [Auth Check] [Rules]  [Error Handling]
              [Error Handler]
```

### Key Architectural Decisions

#### 1. **Dependency Injection**
```go
// ✅ Good: Interface-based dependency
type championshipService struct {
    repo ChampionshipRepository // interface
}

func NewChampionshipService(repo ChampionshipRepository) ChampionshipService {
    return &championshipService{repo: repo}
}
```

#### 2. **Interface Segregation**
- Repository interfaces: `internal/out/database/interfaces.go`
- Service interfaces: `internal/services/interfaces.go`
- Implementations follow `<entity>Repository` and `<entity>Service` naming

#### 3. **Critical Middleware Order**
```go
api.Use(mdlwrs.Otel.Handle)         // 1. Trace generation/extraction
api.Use(mdlwrs.ErrorHandler.Handle) // 2. Centralized error handling
api.Use(mdlwrs.Logger.Handle)       // 3. Request/response logging
```

**Why this order matters**:
- OTEL generates/extracts `trace_id` and stores in context
- ErrorHandler catches panics and errors
- Logger enriches logs with `trace_id` from context

#### 4. **Database Schema Design**

14 tables with normalized structure:

- **Championships**: `championships`, `host_countries`, `championship_hosts`
- **Geography**: `cities`, `championship_cities`
- **Teams**: `teams`, `championship_teams`, `groups`, `team_groups`
- **Matches**: `matches` (with regular/extra time/penalty tracking)
- **Users**: `users`, `sessions`
- **Predictions**: `predictions`
- **Ratings**: `championship_user_ratings`, `user_ratings`

See [DATABASE_SCHEMA.md](docs/summary/DATABASE_SCHEMA.md) for detailed ERD.

---

## 🚀 Getting Started

### Prerequisites

- **Docker** 20.10+ and **Docker Compose** v2.0+
- **Go** 1.25+ (for local development)
- **PostgreSQL** 16+ (for local development without Docker)
- **Make** (optional, for using Makefile targets)

### Quick Start with Docker Compose

#### 1. Clone and Configure

```bash
git clone <repository-url>
cd football-tracker

# Copy environment template
cp .env.example .env

# Edit .env with your configuration
nano .env
```

#### 2. Start All Services

```bash
# Start database, backend, nginx, and observability stack
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f backend
```

#### 3. Apply Database Migrations

```bash
# Wait for PostgreSQL to be ready
sleep 10

# Apply initial migration
docker exec -i football_db psql -U postgres -d footballTracker < migrations/001_init.sql

# Apply session management migration
docker exec -i football_db psql -U postgres -d footballTracker < migrations/002_add_sessions.sql

# Verify tables
docker exec -it football_db psql -U postgres -d footballTracker -c "\dt"
```

#### 4. Verify Installation

```bash
# Health check
curl http://localhost/healthCheck

# API via Nginx (HTTPS)
curl -k https://localhost/api/v1/championships

# Swagger UI
open http://localhost/swagger/index.html

# Grafana dashboards
open http://localhost:3000  # Default: admin/admin
```

### Local Development (Without Docker)

#### 1. Start PostgreSQL

```bash
# macOS with Homebrew
brew install postgresql@16
brew services start postgresql@16

# Create database
createdb footballTracker
```

#### 2. Configure Environment

```bash
# Create .env file
cat > .env << EOF
DATABASE_NAME=footballTracker
DB_HOST=localhost
DB_PORT=5432
DATABASE_USERNAME=postgres
DATABASE_PASSWORD=postgres
SERVER_PORT=8080
LOGGER_LEVEL=debug
OTEL_ENABLED=false  # Disable OTEL for local dev
EOF
```

#### 3. Run Application

```bash
# Install dependencies
go mod download

# Run application
go run cmd/main.go
```

#### 4. Run Tests

```bash
# All tests with race detection
go test -v -race ./...

# Unit tests only
go test -tags=unit -v -race ./internal/services/... ./internal/models/...

# Coverage with HTML report
go test -v -race -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# Unit test coverage
go test -tags=unit -coverprofile=coverage.unit.out -covermode=atomic ./internal/services/... ./internal/models/...
go tool cover -html=coverage.unit.out -o coverage.unit.html
```

### Environment Variables

Key configuration options (see `.env.example` for full list):

```bash
# Server Configuration
SERVER_HOST=localhost
SERVER_PORT=8080
SHUTDOWN_TIMEOUT=30s

# Database
DATABASE_NAME=footballTracker
DB_HOST=postgres  # Use 'localhost' for local dev
DB_PORT=5432
DATABASE_USERNAME=postgres
DATABASE_PASSWORD=<your-password>

# Session Management
SESSION_TOKEN_TTL=24h
SESSION_COOKIE_SECURE=true  # HTTPS only
SESSION_CLEANUP_INTERVAL=1h

# Background Jobs
JOBS_ENABLED=true
JOB_SESSION_CLEANUP_ENABLED=true

# OpenTelemetry
OTEL_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4317
OTEL_SERVICE_NAME=football-tracker

# Logging
LOGGER_LEVEL=info  # debug, info, warn, error
APP_ENV=production
```

---

## 📚 API Documentation

### Interactive Documentation

- **Swagger UI**: `http://localhost/swagger/index.html`
- **OpenAPI JSON**: `http://localhost/swagger/doc.json`

### API Endpoints

#### Health & Monitoring

```
GET /healthCheck        - Liveness probe (always returns 200)
GET /ready              - Readiness probe (503 during shutdown)
```

#### Authentication

```
POST /api/v1/auth/register  - Create new user account
POST /api/v1/auth/login     - Authenticate and get session cookie
POST /api/v1/auth/logout    - Invalidate session
```

**Demo Credentials**:
- Regular User: `demo` / `demo123`
- Admin: `admin` / `admin123`

#### Championships

```
GET /api/v1/championships           - List all championships
GET /api/v1/championships/{id}      - Get championship details
```

#### Teams

```
GET /api/v1/teams/championship/{id} - Get teams for championship
GET /api/v1/teams/{id}              - Get team details
```

#### Matches

```
GET /api/v1/matches/championship/{id}  - Get matches for championship
GET /api/v1/matches/{id}               - Get match details
```

#### Predictions (Requires Authentication)

```
POST /api/v1/predictions  - Create or update prediction
```

**Request Body**:
```json
{
  "match_id": 1,
  "home_score_regular": 2,
  "away_score_regular": 1,
  "home_score_total": 3,        // Optional for playoffs
  "away_score_total": 2,         // Optional for playoffs
  "penalty_winner_team_id": 5    // Optional for playoffs
}
```

### Authentication Flow

1. **Register**: `POST /api/v1/auth/register`
   ```json
   {
     "username": "john_doe",
     "email": "john@example.com",
     "password": "secure_password"
   }
   ```

2. **Login**: `POST /api/v1/auth/login`
   ```json
   {
     "username": "john_doe",
     "password": "secure_password"
   }
   ```
   → Returns `session_token` cookie (HttpOnly, Secure in production)

3. **Authenticated Requests**: Cookie automatically sent by browser

4. **Logout**: `POST /api/v1/auth/logout`

---

## 📊 Observability

### Full Stack Integration

```
Application → OpenTelemetry SDK → OTLP Collector → Tempo/Prometheus
                                                  ↓
Docker Logs → Promtail → Loki ← Grafana → Dashboards
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana** | `http://localhost:3000` | admin/admin |
| **Prometheus** | `http://localhost:9090` | - |
| **Tempo** | `http://localhost:3200` | - |
| **Loki** | `http://localhost:3100` | - |

### Distributed Tracing (W3C Trace Context)

**Automatic Trace Propagation**:
```
Client → Nginx → Backend → Database
   ↓       ↓        ↓         ↓
[trace_id: abc123] [same trace_id across all spans]
```

**OTEL Middleware** automatically:
- Extracts `traceparent` header from incoming requests
- Creates child span with same `trace_id` (or generates new for root span)
- Injects `traceparent` into response headers
- Stores `trace_id` in context for logger enrichment

**Viewing Traces in Grafana**:
1. Navigate to Explore → Tempo
2. Search by `trace_id` (from logs or headers)
3. View full request flow with timing breakdown

### Structured Logging

**Log Format** (JSON in production):
```json
{
  "time": "2026-03-01T10:30:45.123Z",
  "level": "INFO",
  "msg": "Processing match prediction",
  "trace_id": "a1b2c3d4e5f6...",
  "span_id": "f6e5d4c3b2a1...",
  "component": "service",
  "method": "CreatePrediction",
  "user_id": 42,
  "match_id": 15
}
```

**Mandatory Log Attributes**:
- `component`: "handler", "service", or "repository"
- `method`: Function name
- Context-specific fields (user_id, match_id, etc.)

### Metrics (Prometheus)

**Custom Application Metrics**:
- `http.server.shutdown.duration` - Graceful shutdown timing
- `http.server.shutdown.total` - Shutdown events counter

**Infrastructure Metrics**:
- PostgreSQL: Connection pool, query performance
- Nginx: Request rate, response times, error rates
- OTEL Collector: Pipeline throughput

**Grafana Dashboards**:
- Application Performance Monitoring (APM)
- Database Monitoring
- Infrastructure Overview
- Trace-to-Metrics correlation

### Log Aggregation (Loki)

**Query Examples**:
```logql
# All backend logs
{container_name="football_tracker"}

# Error logs only
{container_name="football_tracker"} |= "ERROR"

# Logs for specific trace
{container_name="football_tracker"} |= "a1b2c3d4e5f6"

# Slow database queries
{container_name="football_db"} |= "duration:" | json | duration > 100
```

**Features**:
- Automatic correlation with traces (click trace_id → view trace)
- Real-time log streaming
- Label-based filtering
- Regex and JSON parsing

---

## 🧪 Testing

### Test Coverage

Current coverage: **80%+** (unit tests for services and models)

### Test Structure

```
internal/
  ├── services/
  │   ├── auth_test.go          - 4 test cases
  │   ├── championship_test.go  - 2 test cases
  │   ├── match_test.go         - 2 test cases
  │   ├── prediction_test.go    - 1 test case
  │   ├── team_test.go          - 2 test cases
  │   └── test_setup.go         - Mock repositories
  └── models/
      ├── match_test.go         - Business logic tests
      ├── prediction_test.go    - Points calculation tests
      └── user_test.go          - Role validation tests
```

### Running Tests

```bash
# All tests with race detection
go test -v -race ./...

# Unit tests only
go test -tags=unit -v -race ./internal/services/... ./internal/models/...

# Coverage with HTML report
go test -v -race -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
open coverage.html  # macOS
# OR
xdg-open coverage.html  # Linux

# Unit test coverage with HTML
go test -tags=unit -coverprofile=coverage.unit.out -covermode=atomic ./internal/services/... ./internal/models/...
go tool cover -html=coverage.unit.out -o coverage.unit.html
open coverage.unit.html

# Coverage threshold check (80%)
go test -tags=unit -coverprofile=coverage.unit.out -covermode=atomic ./internal/services/... ./internal/models/...
go tool cover -func=coverage.unit.out | grep total

# Verbose output
go test -v ./...
```

### Test Examples

**Service Test with Mocks** (`auth_test.go`):
```go
func TestAuthService_Register(t *testing.T) {
    mockUserRepo := new(MockUserRepository)
    mockSessionRepo := new(MockSessionRepository)
    authService := NewAuthService(mockUserRepo, mockSessionRepo, 24*time.Hour)
    
    // Setup expectations
    mockUserRepo.On("GetByUsername", mock.Anything, "testuser").Return(nil, nil)
    mockUserRepo.On("Create", mock.Anything, mock.Anything).Return(nil)
    
    // Execute
    resp, err := authService.Register(ctx, dto.RegisterRequest{...})
    
    // Assert
    require.NoError(t, err)
    assert.NotEmpty(t, resp.SessionToken)
}
```

**Model Test** (`prediction_test.go`):
```go
func TestPrediction_CalculatePoints(t *testing.T) {
    tests := []struct {
        name           string
        prediction     Prediction
        match          Match
        expectedPoints int
    }{
        {
            name: "Exact score - 3 points",
            prediction: Prediction{HomeScoreRegular: 2, AwayScoreRegular: 1},
            match: Match{HomeScoreRegular: ptr(2), AwayScoreRegular: ptr(1)},
            expectedPoints: 3,
        },
        // ... more cases
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            points := tt.prediction.CalculatePoints(&tt.match)
            assert.Equal(t, tt.expectedPoints, points)
        })
    }
}
```

### Test Best Practices

- **Table-driven tests** for multiple scenarios
- **Mock repositories** using `testify/mock`
- **Context propagation** for all service methods
- **No external dependencies** (no real database in unit tests)
- **Clear test names** describing scenario and expected outcome

---

## 🔒 Security

### Authentication & Authorization

- **Password Security**: Bcrypt hashing with cost 10
- **Session Management**: Cryptographically secure tokens (UUID v4)
- **Cookie Security**: HttpOnly, Secure (HTTPS), SameSite attributes
- **Role-Based Access Control**: Admin vs. Regular User

### Infrastructure Security

**Nginx Security Headers**:
```
X-Frame-Options: SAMEORIGIN
X-Content-Type-Options: nosniff
X-XSS-Protection: 1; mode=block
Content-Security-Policy: default-src 'self'; ...
Permissions-Policy: geolocation=(), microphone=(), camera=()
```

**Rate Limiting**:
- API endpoints: 10 requests/second per IP
- Auth endpoints: 5 requests/minute per IP
- Connection limiting per IP

**Docker Security**:
- Non-root user (`appuser`) in container
- Multi-stage builds (minimal attack surface)
- Read-only filesystem where possible
- No secrets in images

**Database Security**:
- Parameterized queries (SQL injection prevention)
- Least privilege database user
- Connection pooling with limits
- Password constraints enforced

### HTTPS Configuration

**Production Setup**:
```bash
# Generate SSL certificates with Let's Encrypt
cd nginx/certbot

# Install certbot
apt-get update && apt-get install -y certbot

# Generate certificates (replace your-domain.com with actual domain)
certbot certonly --standalone -d your-domain.com -d www.your-domain.com \
  --non-interactive --agree-tos --email admin@your-domain.com

# Copy certificates to nginx directory
cp /etc/letsencrypt/live/your-domain.com/fullchain.pem ../ssl/
cp /etc/letsencrypt/live/your-domain.com/privkey.pem ../ssl/

# Nginx automatically redirects HTTP → HTTPS
```

**Development Setup**:
```bash
# Create SSL directory if it doesn't exist
mkdir -p nginx/ssl
cd nginx/ssl

# Generate self-signed certificate (valid for 365 days)
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout privkey.pem \
  -out fullchain.pem \
  -subj "/C=US/ST=State/L=City/O=Organization/OU=Department/CN=localhost"

# Verify certificate was created
ls -la *.pem

# Certificate files:
# - fullchain.pem: SSL certificate
# - privkey.pem: Private key
```

---

## 🏗️ Project Structure

```
football-tracker/
├── cmd/
│   ├── main.go                    # Application entry point
│   └── server/
│       ├── config/                # Configuration management
│       ├── handlers/              # HTTP handlers (presentation layer)
│       │   ├── auth/              # Authentication endpoints
│       │   ├── championships/     # Championship endpoints
│       │   ├── matches/           # Match endpoints
│       │   ├── predictions/       # Prediction endpoints
│       │   ├── teams/             # Team endpoints
│       │   └── health/            # Health check endpoints
│       ├── middlewares/           # HTTP middlewares
│       │   ├── otel/              # OpenTelemetry tracing
│       │   ├── logger/            # Request/response logging
│       │   ├── auth/              # Session validation
│       │   └── errorHandler/      # Centralized error handling
│       ├── logger/                # Logger initialization
│       └── server.go              # Server setup and graceful shutdown
├── internal/
│   ├── services/                  # Business logic layer
│   │   ├── interfaces.go          # Service interfaces
│   │   ├── services.go            # DI container
│   │   ├── auth.go                # Authentication logic
│   │   ├── championship.go        # Championship logic
│   │   ├── match.go               # Match logic
│   │   ├── prediction.go          # Prediction logic
│   │   ├── team.go                # Team logic
│   │   └── *_test.go              # Unit tests with mocks
│   ├── out/database/              # Data access layer
│   │   ├── interfaces.go          # Repository interfaces
│   │   ├── repositories.go        # Repository DI container
│   │   ├── championship.go        # Championship repository
│   │   ├── match.go               # Match repository
│   │   ├── prediction.go          # Prediction repository
│   │   ├── session.go             # Session repository
│   │   ├── team.go                # Team repository
│   │   └── user.go                # User repository
│   ├── models/                    # Domain models and DTOs
│   │   ├── championship.go        # Championship model
│   │   ├── match.go               # Match model with business logic
│   │   ├── prediction.go          # Prediction model with points calculation
│   │   ├── user.go                # User model with role validation
│   │   ├── session.go             # Session model
│   │   ├── team.go                # Team model
│   │   ├── dto/                   # Data Transfer Objects
│   │   └── *_test.go              # Model tests
│   ├── clients/                   # External dependencies
│   │   └── database/              # PostgreSQL client
│   ├── telemetry/                 # OpenTelemetry setup
│   │   ├── tracer.go              # Trace configuration
│   │   └── metrics.go             # Metrics configuration
│   ├── jobs/                      # Background jobs
│   │   ├── manager.go             # Job lifecycle management
│   │   └── session_cleaner.go    # Session cleanup job
│   └── testhelpers/               # Test utilities
├── migrations/
│   ├── 001_init.sql               # Initial schema + sample data
│   └── 002_add_sessions.sql       # Session management
├── observability/
│   ├── grafana/                   # Grafana configuration
│   │   ├── datasources.yml        # Prometheus, Loki, Tempo integration
│   │   └── dashboards.yml         # Dashboard provisioning
│   ├── prometheus/
│   │   └── prometheus.yml         # Scrape configs
│   ├── tempo/
│   │   └── tempo.yml              # Trace storage config
│   ├── loki/
│   │   └── loki.yml               # Log aggregation config
│   ├── promtail/
│   │   └── promtail.yml           # Log collection config
│   └── otel-collector/
│       └── otel-collector.yml     # OTLP pipeline config
├── nginx/
│   ├── Dockerfile                 # Nginx container
│   ├── nginx.conf                 # Reverse proxy config (HTTPS, rate limiting)
│   ├── ssl/                       # SSL certificates
│   └── certbot/                   # Let's Encrypt automation
├── docs/
│   ├── swagger.json               # OpenAPI specification
│   └── summary/                   # Analysis documentation
│       ├── ARCHITECTURE_REVIEW.md
│       ├── CODE_QUALITY.md
│       └── DATABASE_SCHEMA.md
├── docker-compose.yml             # Full stack orchestration
├── Dockerfile                     # Multi-stage Go build
├── go.mod                         # Go dependencies
└── .env.example                   # Environment template
```

---

## 📈 Performance Considerations

### Database Optimizations

- **Indexes**: 20+ strategic indexes on frequently queried columns
- **Connection Pooling**: Min 10, Max 100 connections with idle timeout
- **Query Optimization**: All queries use proper WHERE clauses and JOINs
- **No N+1 Queries**: Efficient data loading strategies

### Application Optimizations

- **Fiber Framework**: High-performance HTTP routing
- **Context Propagation**: Efficient request cancellation
- **Graceful Shutdown**: Zero-downtime deployments
- **Background Jobs**: Async session cleanup
- **Structured Logging**: Minimal overhead with slog

### Observability Overhead

- **OTEL Sampling**: Configurable trace sampling (100% by default)
- **Metrics Export**: Batched every 10 seconds
- **Log Aggregation**: Asynchronous with Promtail
- **Disable in Dev**: Set `OTEL_ENABLED=false` for local development

---

## 🤝 Contributing

### Development Workflow

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes with Tests**
   ```bash
   # Write code
   # Add unit tests
   
   # Ensure 80%+ coverage
   go test -tags=unit -coverprofile=coverage.unit.out -covermode=atomic ./internal/services/... ./internal/models/...
   go tool cover -func=coverage.unit.out | grep total
   ```

3. **Commit with Conventional Commits**
   ```
   feat: add user profile endpoint
   fix: correct prediction points calculation
   docs: update API documentation
   test: add integration tests for auth flow
   ```

4. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

### Code Style Guidelines

- Follow **Clean Architecture** principles
- Use **interface-based dependency injection**
- Write **table-driven tests** for all business logic
- Add **structured logging** with mandatory attributes
- Maintain **80%+ test coverage**
- Document **public APIs** with Swagger annotations

### Adding New Endpoints

1. Define DTOs in `internal/models/dto/`
2. Add repository interface in `internal/out/database/interfaces.go`
3. Implement repository in `internal/out/database/<entity>.go`
4. Add service interface in `internal/services/interfaces.go`
5. Implement service with business logic
6. Create handler in `cmd/server/handlers/<entity>/`
7. Register routes in `cmd/server/server.go`
8. Add Swagger annotations
9. Write unit tests

---

## 📖 Additional Documentation

- [Architecture Deep Dive](docs/summary/ARCHITECTURE_REVIEW.md)
- [Code Quality Analysis](docs/summary/CODE_QUALITY.md)
- [Database Schema](docs/summary/DATABASE_SCHEMA.md)
- [API Reference](http://localhost/swagger/index.html)

---

## 🐛 Troubleshooting

### Common Issues

**Problem**: Database connection refused
```bash
# Check PostgreSQL is running
docker-compose ps postgres

# View logs
docker-compose logs postgres

# Restart database
docker-compose restart postgres
```

**Problem**: Migrations not applied
```bash
# Check database exists
docker exec -it football_db psql -U postgres -l

# Re-run migration
docker exec -i football_db psql -U postgres -d footballTracker < migrations/001_init.sql
```

**Problem**: OTEL Collector not receiving traces
```bash
# Check collector logs
docker-compose logs otel-collector

# Verify backend configuration
docker-compose exec backend env | grep OTEL

# Test with OTEL disabled
echo "OTEL_ENABLED=false" >> .env
docker-compose restart backend
```

**Problem**: Nginx 502 Bad Gateway
```bash
# Check through Nginx with HTTPS (from host machine)
curl -k https://localhost/healthCheck
curl -k https://localhost/ready

# Check through Nginx with HTTP (from host machine)
curl http://localhost/healthCheck
curl http://localhost/ready

# View backend logs
docker-compose logs backend

# View Nginx logs
docker-compose logs nginx

# Check if backend container is running
docker-compose ps backend

# Restart backend if needed
docker-compose restart backend
```

---

## 📄 License

Proprietary - All Rights Reserved

This software is the property of the copyright holder and is protected by copyright laws. Unauthorized copying, modification, distribution, or use of this software, via any medium, is strictly prohibited without explicit written permission from the copyright holder.

---

## 👥 Authors & Acknowledgments

**Developed by**: Roman Reznik

### Acknowledgments

- Go community for excellent tooling and libraries
- OpenTelemetry for distributed tracing standards
- Grafana Labs for observability stack
- Fiber framework contributors

---

## 🗺️ Roadmap

### Planned Features

- [ ] **UI**: Web application
- [ ] **Raiting**:Reitings service
- [ ] **Notifications**: Match reminders, leaderboard updates
- [ ] **User Groups**: introduce user groups
- [ ] **Real-time Updates**:  support for live match updates
- [ ] **Admin Panel**: Web-based championship/match management
- [ ] **Multi-language**: i18n support for global audience

### Technical Improvements

- [ ] **Redis Caching**: Reduce database load for read-heavy endpoints
- [ ] **Tests**: Unit and Integation Tests
- [ ] **Load Testing**: Performance benchmarks with k6
- [ ] **Kubernetes Deployment**: Production-ready orchestration (optional)
- [ ] **CI/CD Pipeline**: Automated testing and deployment (optional)
---

**⚡️ Built with Go, PostgreSQL, and modern DevOps practices**
**⚡️ AI - Claude Sonnet  4.5**

