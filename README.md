# Sample Project

A demonstration of using Go Programming Language to implement Server APIs
handling huge request with large input using Go concurrency feature.

## Getting Started

To build the project either using make command or go command.

```bash
make build
```

If you've specific IDE such as GoLand or VSCode, you can configure the build configuration to point to file `cmd/main.go`.


## Running Tests and Benchmarks

Run all unit using make command

```bash
make test
```

or Go command

```bash
go test -race ./...
```

Run all benchmark using make command

```bash
make bench
```

or Go command

```bash
go test -bench=. ./...
```