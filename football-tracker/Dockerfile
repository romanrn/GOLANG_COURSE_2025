FROM golang:1.23-alpine AS builder

# Set GOTOOLCHAIN to allow automatic Go version upgrade
ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY cmd ./cmd
COPY internal ./internal

# Build the server with optimizations
RUN go build -ldflags="-w -s" -o server ./cmd

# Runtime stage
FROM alpine:latest

# Install CA certificates for HTTPS requests and wget for healthcheck
RUN apk --no-cache add ca-certificates wget

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Set working directory
WORKDIR /home/appuser

# Copy binary from builder
COPY --from=builder /app/server ./server

# Set permissions
RUN chmod +x ./server && chown appuser:appgroup ./server

USER appuser

EXPOSE 8080

CMD ["./server"]
