.PHONY: build test run clean lint docker
build:
	go build -o bin/goose ./cmd/goose
test:
	go test -v -race ./...
run:
	go run ./cmd/goose
clean:
	rm -rf bin/ coverage.out
lint:
	go vet ./...
docker:
	docker build -t goose:latest .
