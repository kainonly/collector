# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A queue-based collector for MongoDB time-series data that consumes BSON payloads from NATS JetStream work-queue streams and writes batches into MongoDB on a fixed schedule.

**Module**: `github.com/kainonly/collector/v3`
**Go Version**: 1.24.0

## Common Commands

```bash
# Run locally (requires config/values.yml)
cp config/values.example.yml config/values.yml
go run .

# Build binary
go build -o collector .

# Run with production logging
MODE=release go run .

# Regenerate wire dependency injection (after modifying bootstrap/wire.go)
go run -mod=mod github.com/goforj/wire/cmd/wire ./bootstrap

# Run tests
go test ./...
```

## Architecture

### Data Flow

1. **Producer** (via `transfer` package): Creates named streams in JetStream, persists config to KV, publishes BSON data
2. **Collector Boot**: Loads stream configs from KV bucket, schedules periodic collection tasks, registers states request/reply handler
3. **Runtime Loop**: Fetches messages from consumers at configured intervals, batch inserts to MongoDB, ACKs on success
4. **Dynamic Config**: Watches KV for changes - PUT triggers subscribe/reschedule, DELETE triggers unsubscribe/cleanup

### Package Structure

- **main.go**: Entry point - initializes logger, bootstraps app, blocks on signal
- **bootstrap/**: Dependency injection using Wire (goforj/wire fork) - providers for NATS, MongoDB, JetStream, KV, Scheduler
- **app/**: Core runtime - KV watching, scheduled fetch tasks, MongoDB writes
- **transfer/**: Producer-side library for stream management and BSON publishing
- **common/**: Shared config structs (`Values`, `Inject`, `Nats`, `Database`) and global logger

### Key Types

- `common.Inject`: Central dependency container holding NATS connection, JetStream, KV bucket, MongoDB client/database, scheduler
- `app.App`: Main runtime embedding Inject, manages scheduled jobs via thread-safe generic map `M[K, S]`
- `transfer.Transfer`: Producer helper bound to namespace for stream/KV operations

### Technical Notes

- Uses `bytedance/sonic` for fast JSON/BSON marshaling
- Namespaces cannot contain hyphens (validated in `transfer.New()`)
- MongoDB writes use majority write concern
- Uses `gocron/v2` for job scheduling
- Wire DI requires regeneration when `bootstrap/wire.go` changes
