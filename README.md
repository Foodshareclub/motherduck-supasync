# MotherDuck Sync

A Rust CLI and library for syncing analytics data from PostgreSQL (Supabase) to [MotherDuck](https://motherduck.com/).

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

## Overview

Sync your PostgreSQL/Supabase data to MotherDuck for fast, cost-effective analytics without impacting production.

```
PostgreSQL/Supabase  →  motherduck-sync  →  MotherDuck (DuckDB)
     (source)              (this tool)         (analytics)
```

## Features

- **Incremental sync** - Only sync new/changed records
- **Full sync** - Resync all data when needed
- **Batch processing** - Efficient bulk inserts (10k+ records/sec)
- **Column mapping** - Rename columns between source and target
- **Filtering** - Sync only specific records with WHERE clauses
- **Privacy-first** - Keep schema details in secrets, not in code
- **CI/CD ready** - GitHub Actions workflow included

## Documentation

📚 **[Full Documentation](./docs/README.md)**

| Guide | Description |
|-------|-------------|
| [Getting Started](./docs/getting-started.md) | Installation and first sync |
| [Configuration](./docs/configuration.md) | All configuration options |
| [CLI Reference](./docs/cli-reference.md) | Command-line usage |
| [Database Setup](./docs/database-setup.md) | PostgreSQL and MotherDuck setup |
| [GitHub Actions](./docs/github-actions.md) | CI/CD automation |
| [Troubleshooting](./docs/troubleshooting.md) | Common issues and solutions |

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [CLI Commands](#cli-commands)
- [Troubleshooting](#troubleshooting)

## Installation

```bash
# From source
git clone https://github.com/Foodshareclub/motherduck-sync
cd motherduck-sync
cargo install --path .

# Or build only
cargo build --release
# Binary at: target/release/motherduck-sync
```

## Quick Start

```bash
# 1. Test connectivity
motherduck-sync test

# 2. Check what needs syncing
motherduck-sync status

# 3. Run incremental sync
motherduck-sync sync

# 4. Full sync (all records)
motherduck-sync sync --full
```


## Configuration

### Step 1: Create tables.local.json

Copy the example file and customize it with your actual table names:

```bash
cp tables.example.json tables.local.json
```

Edit `tables.local.json` with your schema:

```json
[
  {
    "source": "analytics_staging_users",
    "target": "full_users",
    "pk": ["id"],
    "columns": ["id", "nickname", "email", "created_at", "updated_at"],
    "order_by": "created_at"
  },
  {
    "source": "analytics_staging_posts",
    "target": "full_listings",
    "pk": ["id"],
    "columns": ["id", "profile_id", "post_name", "post_type", "created_at"],
    "mappings": {"profile_id": "user_id"},
    "order_by": "created_at"
  },
  {
    "source": "analytics_daily_stats",
    "target": "daily_stats",
    "pk": ["date"],
    "order_by": "date"
  }
]
```

> **Important**: `tables.local.json` is gitignored to keep your schema private. Never commit it!

### Step 2: Generate the Secret

Use the built-in command to generate a base64-encoded secret:

```bash
motherduck-sync generate-secret --input tables.local.json
```

Output:
```
=== SYNC_TABLES_CONFIG Secret ===

W3sic291cmNlIjoiYW5hbHl0aWNzX3N0YWdpbmdfdXNlcnMi...

=== Instructions ===
1. Go to GitHub repo → Settings → Secrets → Actions
2. Create/update secret: SYNC_TABLES_CONFIG
3. Paste the value above
```

**Alternative (manual):**

```bash
# macOS/Linux
cat tables.local.json | base64

# Or minify first (recommended)
cat tables.local.json | jq -c | base64
```

### Step 3: Set Up GitHub Secrets

Go to your repository: **Settings → Secrets and variables → Actions**

Add these secrets:

| Secret | Description | Example |
|--------|-------------|---------|
| `DATABASE_URL` | Supabase pooler connection string | `postgres://postgres.xxx:password@aws-0-region.pooler.supabase.com:6543/postgres` |
| `MOTHERDUCK_TOKEN` | MotherDuck access token | `eyJ...` |
| `SYNC_TABLES_CONFIG` | Base64-encoded table config | (output from step 2) |

#### Getting Your Credentials

**Supabase DATABASE_URL:**
1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project → Settings → Database
3. Copy "Connection string" under **Connection pooling** (port 6543)
4. Replace `[YOUR-PASSWORD]` with your database password

> ⚠️ Use the **pooler** connection (port 6543), not direct (5432)

**MotherDuck Token:**
1. Go to [app.motherduck.com](https://app.motherduck.com)
2. Settings → Access Tokens → Create Token
3. Copy the token

### Step 4: Local Development

For local development, set environment variables:

```bash
# Required
export DATABASE_URL="postgres://postgres.PROJECT:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres?sslmode=require"
export MOTHERDUCK_TOKEN="your_token_here"

# Option A: Use plain JSON (easier for local dev)
export SYNC_TABLES_JSON='[{"source":"my_table","target":"target","pk":["id"]}]'

# Option B: Use base64 (same as CI)
export SYNC_TABLES_CONFIG="$(cat tables.local.json | base64)"
```

Or create a `.env` file (gitignored):

```bash
DATABASE_URL=postgres://...
MOTHERDUCK_TOKEN=...
SYNC_TABLES_JSON=[{"source":"my_table","target":"target","pk":["id"]}]
```


## CLI Commands

```
motherduck-sync <COMMAND>

Commands:
  sync             Sync data to MotherDuck (default)
  test             Test connectivity to both databases
  status           Show unsynced record counts
  query            Query MotherDuck tables
  clean            Truncate or reset tables
  init             Generate sample TOML config file
  generate-secret  Generate base64 secret from JSON file

Global Options:
  --full           Full sync (resync all records)
  --config <FILE>  Config file path
  --json           JSON output format
  -q, --quiet      Minimal output
  --log-level      Log level (default: info)
```

### Examples

```bash
# Sync with progress
motherduck-sync sync

# Full resync
motherduck-sync sync --full

# Test connectivity
motherduck-sync test

# Check unsynced counts
motherduck-sync status

# Query synced data
motherduck-sync query --sql "SELECT COUNT(*) FROM full_users"

# Show all table counts
motherduck-sync query --counts

# List tables in MotherDuck
motherduck-sync query --tables

# Reset and resync
motherduck-sync clean --truncate
motherduck-sync sync --full

# Generate TOML config file
motherduck-sync init --output my-config.toml

# Generate secret from custom file
motherduck-sync generate-secret --input my-tables.json
```

## Table Configuration Reference

### JSON Format (for SYNC_TABLES_CONFIG)

```json
[
  {
    "source": "source_table_name",
    "target": "target_table_name",
    "pk": ["id"],
    "columns": ["id", "name", "created_at"],
    "mappings": {"old_column": "new_column"},
    "order_by": "created_at",
    "filter": "status = 'active'",
    "enabled": true
  }
]
```

### Field Reference

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `source` | ✅ | - | Source table name in PostgreSQL |
| `target` | ✅ | - | Target table name in MotherDuck |
| `pk` | ✅ | - | Primary key column(s) as array |
| `columns` | ❌ | all | Columns to sync (empty = all columns) |
| `mappings` | ❌ | {} | Column renames: `{"source": "target"}` |
| `order_by` | ❌ | null | ORDER BY column for consistent ordering |
| `filter` | ❌ | null | WHERE clause filter (without WHERE keyword) |
| `enabled` | ❌ | true | Enable/disable this table |

### Configuration Examples

**Basic table:**
```json
{"source": "users", "target": "users", "pk": ["id"]}
```

**With column selection:**
```json
{
  "source": "users",
  "target": "analytics_users",
  "pk": ["id"],
  "columns": ["id", "email", "created_at"]
}
```

**With column mapping:**
```json
{
  "source": "posts",
  "target": "listings",
  "pk": ["id"],
  "mappings": {"profile_id": "user_id", "post_name": "title"}
}
```

**With filter:**
```json
{
  "source": "orders",
  "target": "completed_orders",
  "pk": ["id"],
  "filter": "status = 'completed' AND created_at > '2024-01-01'"
}
```

**Composite primary key:**
```json
{"source": "user_roles", "target": "user_roles", "pk": ["user_id", "role_id"]}
```

**Disabled table:**
```json
{"source": "legacy_table", "target": "legacy", "pk": ["id"], "enabled": false}
```


## GitHub Actions

The included workflow (`.github/workflows/sync.yml`) runs automatically:

```yaml
on:
  schedule:
    - cron: '0 3 * * *'   # Daily at 3 AM UTC
    - cron: '0 11 * * *'  # Daily at 11 AM UTC
    - cron: '0 19 * * *'  # Daily at 7 PM UTC
  workflow_dispatch:       # Manual trigger
```

### Workflow Setup

1. Add the three secrets (`DATABASE_URL`, `MOTHERDUCK_TOKEN`, `SYNC_TABLES_CONFIG`)
2. The workflow runs automatically on schedule
3. Or trigger manually: Actions → Sync → Run workflow

### Manual Trigger Options

When triggering manually, you can choose:
- **Sync mode**: `incremental` (default) or `full`
- **Log level**: `info`, `debug`, or `warn`

## Database Setup

### Supabase Source Tables

Create staging tables with a sync tracking column:

```sql
-- Example: Users staging table
CREATE TABLE analytics_staging_users (
    id text PRIMARY KEY,
    nickname text,
    email text,
    created_at timestamptz,
    updated_at timestamptz,
    synced_to_motherduck boolean DEFAULT false
);

-- Index for efficient unsynced queries
CREATE INDEX idx_staging_users_unsynced 
ON analytics_staging_users(synced_to_motherduck) 
WHERE NOT synced_to_motherduck;
```

### Populate from Source Tables

```sql
-- Option 1: View (for read-only sync)
CREATE VIEW analytics_staging_users AS
SELECT
    id::text,
    nickname,
    email,
    created_time as created_at,
    updated_at,
    false as synced_to_motherduck
FROM profiles;

-- Option 2: Trigger (for incremental sync with marking)
CREATE OR REPLACE FUNCTION sync_to_staging()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO analytics_staging_users (id, nickname, email, created_at, updated_at)
    VALUES (NEW.id::text, NEW.nickname, NEW.email, NEW.created_time, NEW.updated_at)
    ON CONFLICT (id) DO UPDATE SET
        nickname = EXCLUDED.nickname,
        email = EXCLUDED.email,
        updated_at = EXCLUDED.updated_at,
        synced_to_motherduck = false;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### MotherDuck Target Tables

Tables are auto-created on first sync. Or create manually:

```sql
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

CREATE TABLE full_users (
    id VARCHAR PRIMARY KEY,
    nickname VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

## Architecture

```
┌──────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    PostgreSQL    │────▶│ motherduck-sync │────▶│   MotherDuck    │
│    (Supabase)    │     │                 │     │    (DuckDB)     │
└──────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │
         │                       ▼
         │               ┌───────────────┐
         └──────────────▶│ Mark as synced│
                         └───────────────┘
```

### Sync Flow

1. **Fetch** unsynced records (`WHERE synced_to_motherduck = false`)
2. **Transform** columns based on mappings
3. **Batch insert** to MotherDuck (1000 records per batch)
4. **Mark** source records as synced

### Performance

- ~10,000 records/second throughput
- Default batch size: 1,000 records
- Full sync of 12,865 records: ~19 seconds

## TOML Configuration (Alternative)

For complex setups, use a TOML config file:

```bash
motherduck-sync init --output motherduck-sync.toml
```

```toml
[postgres]
url = "postgres://user:password@localhost:5432/database"
pool_size = 5

[motherduck]
token = "your_motherduck_token"
database = "analytics"

[sync]
batch_size = 1000
mark_synced = true

[[tables]]
source_table = "analytics_staging_users"
target_table = "full_users"
primary_key = ["id"]
enabled = true
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Use Supabase pooler (port 6543), not direct (5432) |
| SSL errors | Add `?sslmode=require` to connection string |
| No tables configured | Check `SYNC_TABLES_CONFIG` is set and base64 valid |
| Slow sync | Add index on `synced_to_motherduck` column |

### Debug Mode

```bash
motherduck-sync --log-level debug sync
```

### Verify Configuration

```bash
# Decode and check your secret
echo $SYNC_TABLES_CONFIG | base64 -d | jq .

# Test connectivity
motherduck-sync test
```

## Privacy & Security

- **Schema privacy**: Table/column names stored in secrets, not in code
- **Credentials**: Never logged or exposed in output
- **Redacted logs**: Table names redacted in CI logs

## License

MIT OR Apache-2.0
