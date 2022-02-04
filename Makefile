
build:
	go build -race -o sample cmd/main.go

test:
	go test -race ./...

bench:
	go test -bench=. ./...