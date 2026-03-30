# Repository Guidelines

## Project Structure & Module Organization

This repository is a small Go library with a flat layout. Production code lives at the repository root in the `memebridge` package, primarily in `meme_to_sppb_value.go`, `memestr_to_sppb_value.go`, and `interval.go`. Tests sit beside the code in `*_test.go` files and use the external `memebridge_test` package. Keep new files at the root unless a new subpackage clearly improves reuse or separation.

## Build, Test, and Development Commands

Use the checked-in `Makefile` for the standard workflow:

- `make test`: runs `go test -v ./...` across the module.
- `make lint`: runs `golangci-lint run`.
- `go test ./...`: quick compile-and-test pass without verbose output.
- `go test -run TestParseExpr ./...`: focused iteration on a single test.

There is no standalone binary build target; this project is validated through tests and linting.

## Coding Style & Naming Conventions

Follow standard Go formatting with `gofmt` and idiomatic imports. Use tabs for indentation as produced by `gofmt`. Exported identifiers use `PascalCase` (`MemefishExprToGCV`); internal helpers use `camelCase` (`astStructLiteralsToGCV`). Prefer precise, behavior-oriented filenames and keep related tests in matching `*_test.go` files. Keep functions small and return explicit errors instead of hiding failures.

## Testing Guidelines

Add table-driven tests for new literal or type-conversion cases. Name tests with the Go convention `TestXxx`, and keep focused subcases under `t.Run(...)` using the input SQL or type as the case name. For parser and conversion changes, cover both successful conversions and edge cases such as `NULL`, typed casts, and interval variants.

## Commit & Pull Request Guidelines

Recent history uses short, imperative subjects such as `Support IntervalLiteralRange` and `Use mainline of memefish`. Keep commit titles concise, sentence case, and action-first. Pull requests should describe the behavior change, note any dependency updates, link the relevant issue when available, and include the commands you ran, typically `make test` and `make lint`. Screenshots are not needed for this library unless documentation output changes.
