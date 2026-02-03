# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod and download dependencies
COPY go.mod ./
RUN go mod download

# Copy source code
COPY cmd/server/ ./cmd/server/
COPY internal/ ./internal/


# Build the server
RUN go build -o server ./cmd/server

# Runtime stage
FROM alpine:latest

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy binary from builder
COPY --from=builder /app/server /usr/local/bin/server

# Switch to non-root user
USER appuser


EXPOSE 8080

CMD ["server"]
