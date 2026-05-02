# Repository Guidelines

## Project Structure & Module Organization

This repository is a small Go library with a flat layout. Production code lives at the repository root in the `memebridge` package, primarily in `meme_to_sppb_value.go`, `memestr_to_sppb_value.go`, and `interval.go`. Tests sit beside the code in `*_test.go` files and use the external `memebridge_test` package. Keep new files at the root unless a new subpackage clearly improves reuse or separation.

## Build, Test, and Development Commands

Use the checked-in `Makefile` for the standard workflow:

- `make test`: runs `go test -v ./...` across the module.
- `make lint`: runs `golangci-lint run`.
- `go test ./...`: quick compile-and-test pass without verbose output.
- `go test -run '^TestParseExpr$' ./...`: focused iteration on the main expression-conversion test.

There is no standalone binary build target; this project is validated through tests and linting.

## Architecture

- `ParseExpr` in `memestr_to_sppb_value.go` is the string entry point: it parses with `memefish.ParseExpr` and delegates to `MemefishExprToGCV`.
- `meme_to_sppb_value.go` is the main AST-to-`spanner.GenericColumnValue` layer. It handles literal evaluation, typed STRUCT/ARRAY expected-type coercion, and array element type inference.
- `cast.go` is the single place for explicit `CAST` / `SAFE_CAST` behavior, typed NULL propagation, scalar conversion rules, ARRAY/STRUCT cast rules, numeric parsing, and temporal conversion defaults.
- `meme_to_sppb_type.go` maps `memefish` types to `spannerpb.Type`; coercion and cast logic relies on those exact type objects.
- `interval.go` contains INTERVAL literal parsing and normalization helpers shared by expression conversion and cast behavior.

## Coding Style & Naming Conventions

Follow standard Go formatting with `gofmt` and idiomatic imports. Use tabs for indentation as produced by `gofmt`. Exported identifiers use `PascalCase` (`MemefishExprToGCV`); internal helpers use `camelCase` (`astStructLiteralsToGCV`). Prefer precise, behavior-oriented filenames and keep related tests in matching `*_test.go` files. Keep functions small and return explicit errors instead of hiding failures.

## Testing Guidelines

Add table-driven tests for new literal or type-conversion cases. Name tests with the Go convention `TestXxx`, and keep focused subcases under `t.Run(...)` using the input SQL or type as the case name. For parser and conversion changes, cover both successful conversions and edge cases such as `NULL`, typed casts, and interval variants.

Tests in this repository use the external `memebridge_test` package. Temporal behavior is expected to match Cloud Spanner's default time zone, `America/Los_Angeles`; tests blank-import `time/tzdata`, and production builds can opt into embedded tzdata with the `memebridge_tzdata` build tag.

Treat Cloud Spanner as the semantic source of truth for cast/coercion behavior. If emulator behavior and library behavior disagree, validate questionable cases against real Spanner before changing `memebridge`.

`spanner-mycli` validation is local/manual only. Do not add emulator or `spanner-mycli` harnesses under `*_test.go`, do not add module dependencies for them, and never commit connection details.

For manual STRUCT validation in Spanner, bare `STRUCT` cannot be returned directly. Use `SELECT [STRUCT(...)]` for success cases, and use `DESCRIBE SELECT [STRUCT(...)]` when nested STRUCT field/type names matter.

## Commit & Pull Request Guidelines

Recent history uses short, imperative subjects such as `Support IntervalLiteralRange` and `Use mainline of memefish`. Keep commit titles concise, sentence case, and action-first. Pull requests should describe the behavior change, note any dependency updates, link the relevant issue when available, and include the commands you ran, typically `make test` and `make lint`. Screenshots are not needed for this library unless documentation output changes.

When changing cast or coercion semantics, consult Cloud Spanner conversion rules and the upstream GoogleSQL cast tables (`googlesql/public/cast.cc` and `googlesql/public/cast.h`), then record which reference justified which behavior change in the PR description or review notes.

For bot review loops in this repository, start with `gh copilot-review request <pr> --wait --timeout 900 --interval 60` to reduce GitHub API churn. If that wait times out, continue with `gh copilot-review check <pr> --interval 60` instead of hand-rolled polling. If GitHub API or rate-limit issues block the review loop, stop and ask the user rather than improvising around the failure.
