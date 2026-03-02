# Agent Guidelines for kafka-data-keep

## Project Overview

Go service that backs up and restores Kafka topic data and consumer group
offsets to/from S3 using Avro OCF format. Single binary with five subcommands:
`topics-backup`, `topics-plan-restore`, `topics-restore`,
`consumergroups-backup`, `consumergroups-restore`.

Module: `github.com/utilitywarehouse/kafka-data-keep`

## Build / Lint / Test Commands

```bash
make build   # CGO_ENABLED=0 go build -o kafka-data-keep ./cmd/main.go
make test    # go test -v --race -cover ./...
make lint    # golangci-lint run --fix
make mod     # go mod tidy
make clean   # rm -f kafka-data-keep
make all     # clean + lint + test + build
```

### Running a Single Test

```bash
# Single package
go test -v ./internal/topics/backup/...

# Single test function
go test -v -run TestPlanner_filterTopics ./internal/topics/planrestore/

# Single sub-test
go test -v -run TestBackupIntegration/multiple_batches ./internal/topics/backup/

# Skip integration tests (requires Docker/testcontainers)
go test -short ./...
```

Integration tests are guarded with:
```go
if testing.Short() {
    t.Skip("Skipping integration test in short mode")
}
```

Integration tests spin up real Redpanda (Kafka) and MinIO (S3) containers
via `testcontainers-go`. Shared helpers live in `internal/testutil/`.

## Code Style

### Formatting

- Standard Go formatting enforced by `gofmt`, `gofumpt`, and `goimports`.
- Run `make lint` (golangci-lint with `--fix`) before committing.
- Indentation: tabs (standard Go).
- Strings: double quotes. Raw strings with backticks for regex literals.
- Opening brace always on the same line (standard Go).
- Octal literals use `0o` prefix: `0o750`, `0o600`.
- Use `fmt.Appendf(nil, ...)` instead of `[]byte(fmt.Sprintf(...))`.

### Imports

Three-group ordering enforced by `gci` and `goimports`:

```go
import (
    // 1. Standard library
    "context"
    "fmt"
    "log/slog"

    // 2. Third-party
    "github.com/twmb/franz-go/pkg/kgo"
    "golang.org/x/sync/errgroup"

    // 3. Internal (same module)
    "github.com/utilitywarehouse/kafka-data-keep/internal"
    ints3 "github.com/utilitywarehouse/kafka-data-keep/internal/s3"
)
```

Use aliases only to resolve naming conflicts; prefer descriptive aliases over
generic ones (`ints3`, `internalkafka`, `topicsbackup`).

Embed with blank import:
```go
import _ "embed"

//go:embed schema.json
var schema string
```

### Naming Conventions

- **Files:** `snake_case.go` — e.g., `partition_writer.go`, `read_latest.go`,
  `backup_integration_test.go`.
- **Packages:** short, lowercase, single-word — `backup`, `restore`,
  `planrestore`, `codec`, `avro`, `testutil`.
- **Exported types/functions:** `PascalCase` — `AppConfig`, `GroupWriter`,
  `NewUploader`, `HandleFetches`.
- **Unexported types:** `camelCase` — `partitionWriter`, `planner`,
  `kafkaRecord`, `writerConfig`.
- **Constructors:** `New<Type>` — `NewUploader`, `NewGroupWriter`,
  `NewRestorer`.
- **Constants:** unexported `camelCase` — `sourceOffsetHeader`, `opsAddr`,
  `maxPollRecords`.
- **Test functions:** `Test<Subject>` or `Test<Subject>Integration`.
- **Test helpers:** unexported within the test file — `stopApp`,
  `writeRecords`, `listFilesOnBucket`.
- Each package's entry point is `AppConfig` + `Run(ctx, cfg)` in `app.go`.

### Types and Structs

- Struct fields always initialized with named fields, never positionally.
- Keep interfaces small; one or two methods per interface where possible.
- Use factory interfaces for codec abstraction (`RecordEncoderFactory`,
  `RecordDecoderFactory`) to enable testability.
- Optional struct fields use pointer types (`*int32`, `*string`).
- Struct tags for Avro: `avro:"field_name"`.
- Pointer receivers on all methods of a struct type.

### Error Handling

Wrap errors with context using `%w`:
```go
return fmt.Errorf("failed to create kafka client: %w", err)
```

Use `errors.Is` for sentinel checks, `errors.As` for structured error types.

Collect multiple errors from loops with `errors.Join`:
```go
var errs []error
for _, w := range writers {
    if err := w.Close(); err != nil {
        errs = append(errs, err)
    }
}
return errors.Join(errs...)
```

Use named return values to capture deferred cleanup errors:
```go
func (u *Uploader) Upload(ctx context.Context, path, key string) (err error) {
    f, err := os.Open(path)
    defer func() {
        if closeErr := f.Close(); closeErr != nil && err == nil {
            err = fmt.Errorf("failed to close file: %w", closeErr)
        }
    }()
```

Log non-fatal deferred errors rather than silently discarding them:
```go
defer func() {
    if err := f.Close(); err != nil {
        slog.ErrorContext(ctx, "failed closing file", "error", err)
    }
}()
```

Validate required config fields at the start of `Run`:
```go
if cfg.S3Bucket == "" {
    return fmt.Errorf("bucket must be provided")
}
```

### Concurrency

- Use `golang.org/x/sync/errgroup` for goroutine fan-out with error
  propagation.
- Derive child context from `errgroup.WithContext` so one goroutine failure
  cancels siblings.
- Protect mutable struct state with `sync.Mutex`; lock at the smallest scope.
- Graceful shutdown via `signal.NotifyContext` with `SIGINT`/`SIGTERM`.

### Logging

Use `log/slog` (stdlib) with context-aware calls throughout:
```go
slog.InfoContext(ctx, "starting backup", "topic", topic, "partition", p)
slog.ErrorContext(ctx, "upload failed", "error", err)
```

Pass key-value pairs as alternating strings, or use `slog.String(key, val)`
typed helpers for clarity.

### Inline Suppression

Use `//nolint:<linter> // <reason>` with a mandatory reason comment:
```go
//nolint:errcheck // only fails if franz-go internals change; caught by integration tests
//nolint:gosec // partition count is bounded to int32 range
```

### Tests

- Use `github.com/stretchr/testify/require` for fatal assertions (stops test
  immediately) and `assert` for non-fatal checks.
- Table-driven tests for unit test cases.
- Call `t.Parallel()` in integration tests and in table-driven sub-tests.
- Use `t.Context()` (Go 1.21+) instead of `context.Background()` in tests.
- Use `t.TempDir()` for temporary directories; cleaned up automatically.
- Generate unique resource names with `uuid` to avoid test cross-contamination.

## Architecture Notes

- `cmd/main.go` dispatches subcommands via `switch os.Args[1]`; each has its
  own `AppConfig` and `Run` in `internal/<domain>/<subcommand>/app.go`.
- CLI flags fall back to env vars via `getEnv*` helpers defined in each
  `app.go`.
- All business logic is under `internal/`; nothing is exported to external
  consumers.
- Avro OCF schemas are embedded at compile time with `//go:embed schema.json`.
- Operational health server runs on `:8081` via `go-operational`.
- OTel metrics exported via `uwos-go/telemetry`; meter initialized at package
  scope.
