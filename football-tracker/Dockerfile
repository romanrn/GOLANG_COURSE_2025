FROM golang:1.23-alpine AS builder

# Set GOTOOLCHAIN to allow automatic Go version upgrade
ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
ENV GOPATH=/go

WORKDIR /app

# Copy dependency files first
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY cmd ./cmd
COPY internal ./internal

# Copy pre-generated Swagger docs (generate locally before building)
COPY docs ./docs

# Build the server with optimizations
RUN go build -ldflags="-w -s" -o server ./cmd/main.go

# Runtime stage
FROM alpine:latest

# Install CA certificates for HTTPS requests and wget for healthcheck
RUN apk --no-cache add ca-certificates wget

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Set working directory
WORKDIR /home/appuser

# Copy binary and docs from builder
COPY --from=builder /app/server ./server
COPY --from=builder /app/docs ./docs

# Set permissions
RUN chmod +x ./server && chown -R appuser:appgroup ./

USER appuser

EXPOSE 8080

CMD ["./server"]
