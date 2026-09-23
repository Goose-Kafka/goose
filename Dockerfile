# Stage 1: Build
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /goose ./cmd/goose

# Stage 2: Runtime (distroless for minimal size)
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /goose /goose

ENTRYPOINT ["/goose"]
