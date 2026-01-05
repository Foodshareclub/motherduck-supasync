# MotherDuck Sync

A Rust CLI for syncing PostgreSQL data to [MotherDuck](https://motherduck.com/) for analytics.

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

```
PostgreSQL/Supabase  →  motherduck-supasync  →  MotherDuck (DuckDB)
```

## Features

- **Incremental sync** — Only sync new/changed records
- **Batch processing** — ~10k records/sec throughput
- **Column mapping** — Rename columns between source and target
- **Privacy-first** — Schema details stored in secrets, not code
- **CI/CD ready** — GitHub Actions workflow included

## Quick Start

```bash
# Install
cargo install --path .

# Set credentials
export DATABASE_URL="postgres://...@pooler.supabase.com:6543/postgres"
export MOTHERDUCK_TOKEN="your_token"
export SYNC_TABLES_JSON='[{"source":"users","target":"users","pk":["id"]}]'

# Sync
motherduck-supasync sync
```

## Documentation

| Guide | Description |
|-------|-------------|
| **[Getting Started](docs/getting-started.md)** | Installation and first sync |
| **[Configuration](docs/configuration.md)** | Environment variables, JSON, and TOML options |
| **[CLI Reference](docs/cli-reference.md)** | All commands and options |
| **[Database Setup](docs/database-setup.md)** | PostgreSQL staging tables and MotherDuck setup |
| **[GitHub Actions](docs/github-actions.md)** | Automated sync workflow |
| **[Troubleshooting](docs/troubleshooting.md)** | Common issues and solutions |

## Configuration

### 1. Create Table Config

```bash
cp tables.example.json tables.local.json
```

```json
[
  {
    "source": "staging_users",
    "target": "full_users",
    "pk": ["id"],
    "columns": ["id", "email", "created_at"],
    "order_by": "created_at"
  }
]
```

### 2. Generate Secret

```bash
motherduck-supasync generate-secret --input tables.local.json
```

### 3. Set GitHub Secrets

| Secret | Description |
|--------|-------------|
| `DATABASE_URL` | Supabase pooler connection (port 6543) |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |
| `SYNC_TABLES_CONFIG` | Base64-encoded table config |

## Commands

```bash
motherduck-supasync sync              # Incremental sync
motherduck-supasync sync --full       # Full resync
motherduck-supasync test              # Test connectivity
motherduck-supasync status            # Show unsynced counts
motherduck-supasync query --counts    # Query MotherDuck
motherduck-supasync generate-secret   # Generate config secret
```

## Table Config Fields

| Field | Required | Description |
|-------|----------|-------------|
| `source` | ✅ | Source table in PostgreSQL |
| `target` | ✅ | Target table in MotherDuck |
| `pk` | ✅ | Primary key column(s) |
| `columns` | | Columns to sync (default: all) |
| `mappings` | | Column renames `{"old": "new"}` |
| `filter` | | WHERE clause filter |
| `order_by` | | ORDER BY column |
| `enabled` | | Enable/disable (default: true) |

## Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌─────────────┐
│  PostgreSQL  │────▶│ motherduck-supasync │────▶│ MotherDuck  │
└──────────────┘     └─────────────────┘     └─────────────┘
       │                     │
       └─────────────────────┘
              Mark synced
```

1. Fetch unsynced records (`WHERE synced_to_motherduck = false`)
2. Batch insert to MotherDuck (1000 records/batch)
3. Mark source records as synced

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

## Contributing

Contributions welcome! Please read the license terms before contributing.
