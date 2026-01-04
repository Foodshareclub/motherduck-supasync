# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Rust CLI and library for syncing analytics data from PostgreSQL (Supabase) to MotherDuck (cloud DuckDB). Syncs records incrementally using a `synced_to_motherduck` boolean column, with support for full re-syncs.

## Build & Test Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build (optimized, LTO enabled)

# Run CLI
cargo run -- <command>         # Run with cargo
motherduck-sync <command>      # After cargo install --path .

# Tests
cargo test                     # Run all tests
cargo test <test_name>         # Run specific test
cargo test --lib               # Library tests only

# Benchmarks
cargo bench                    # Run sync_benchmark

# Lint/Format
cargo fmt                      # Format code
cargo clippy                   # Lint
```

## Architecture

```
src/
├── bin/main.rs      # CLI entry point (clap-based commands)
├── lib.rs           # Public API re-exports
├── config.rs        # SyncConfig, TableMapping, environment loading
├── sync.rs          # SyncClient - orchestrates sync operations
├── postgres.rs      # PostgresClient - source database operations
├── motherduck.rs    # MotherDuckClient - target database operations
├── schema.rs        # Column/table schema types
├── error.rs         # Error types (thiserror)
└── metrics.rs       # Observability metrics
```

**Core Flow:**
1. `SyncClient::new()` connects to both PostgreSQL and MotherDuck
2. `sync()` iterates over enabled `TableMapping` configs
3. `PostgresClient::fetch_rows()` queries unsynced records (`WHERE NOT synced_to_motherduck`)
4. `MotherDuckClient::batch_upsert()` inserts to DuckDB in transactions
5. `PostgresClient::mark_synced()` updates source records

## Configuration System

Three config sources (in order of precedence):
1. **TOML file**: `motherduck-sync.toml` or `--config path`
2. **Environment variables**: `DATABASE_URL`, `MOTHERDUCK_TOKEN`
3. **Table configs**: `SYNC_TABLES_CONFIG` (base64-encoded JSON) or `SYNC_TABLES_JSON` (plain JSON for local dev)

Generate secrets: `motherduck-sync generate-secret --input tables.local.json`

## Key Types

- `SyncConfig` - main config with postgres/motherduck/sync/tables sections
- `TableMapping` - source→target table config with column mappings, filters
- `SyncClient` - main entry point for sync operations
- `SyncMode::Incremental | Full` - sync modes
- `SyncResult` / `TableSyncResult` - structured sync results

## Features (Cargo)

- `cli` (default): clap, indicatif, console for CLI
- `tls-native` (default): native-tls for Supabase SSL
- `tls-rustls`: alternative TLS backend

## Important Implementation Details

- Uses `simple_query` for PostgreSQL fetch (returns all values as strings)
- DuckDB bulk inserts use `INSERT OR REPLACE INTO ... VALUES` syntax
- Batch size default: 1000 records
- Passwords are masked in logs (`mask_url()`)
- Table names are redacted in CI logs for privacy
