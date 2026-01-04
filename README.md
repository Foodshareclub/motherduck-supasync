# MotherDuck Sync

A Rust CLI for syncing analytics data from Supabase PostgreSQL to [MotherDuck](https://motherduck.com/).

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

## Overview

Sync your Supabase data to MotherDuck for fast, cost-effective analytics without impacting production.

```
Supabase PostgreSQL  →  motherduck-sync  →  MotherDuck (DuckDB)
     (source)              (this tool)         (analytics)
```

## Installation

```bash
# From source
git clone https://github.com/Foodshareclub/motherduck-sync
cd motherduck-sync
cargo install --path .

# Or build only
cargo build --release
```

## Quick Start

```bash
# Test connectivity
motherduck-sync test

# Run incremental sync
motherduck-sync sync

# Full sync (all records)
motherduck-sync sync --full

# Check table counts
motherduck-sync query --counts
```

## Configuration

### GitHub Secrets (Required)

Set these in your repository settings → Secrets → Actions:

| Secret | Description |
|--------|-------------|
| `DATABASE_URL` | Supabase pooler connection string |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |
| `SYNC_TABLES_CONFIG` | Base64-encoded JSON table config (keeps schema private) |

### Table Configuration

Table and column names are stored in `SYNC_TABLES_CONFIG` secret to keep schema details out of the public repo.

**Format**: Base64-encoded JSON array:

```json
[
  {
    "source": "staging_users",
    "target": "full_users", 
    "pk": ["id"],
    "columns": ["id", "name", "email", "created_at"],
    "order_by": "created_at"
  },
  {
    "source": "staging_posts",
    "target": "full_posts",
    "pk": ["id"],
    "mappings": {"profile_id": "user_id"}
  }
]
```

**To create the secret**:
```bash
# Create your config JSON, then base64 encode it
cat config.json | base64
# Paste the output as SYNC_TABLES_CONFIG secret value
```

**Config fields**:
- `source` - Source table in PostgreSQL (required)
- `target` - Target table in MotherDuck (required)
- `pk` - Primary key column(s) (required)
- `columns` - Columns to sync (optional, empty = all)
- `mappings` - Column renames: source → target (optional)
- `order_by` - Order by column (optional)
- `filter` - WHERE clause filter (optional)
- `enabled` - Enable/disable table (default: true)

### Environment Variables (Local Dev)

```bash
# Supabase connection (use pooler, port 6543)
export DATABASE_URL="postgres://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres?sslmode=require"

# MotherDuck token (from https://app.motherduck.com)
export MOTHERDUCK_TOKEN="your_token"

# For local dev, use plain JSON instead of base64
export SYNC_TABLES_JSON='[{"source":"my_table","target":"target","pk":["id"]}]'
```

### Getting Your Credentials

**Supabase DATABASE_URL:**
1. Supabase Dashboard → Project Settings → Database
2. Copy "Connection string" under "Connection pooling" (port 6543)
3. Replace `[YOUR-PASSWORD]` with your database password

**MotherDuck Token:**
1. Go to https://app.motherduck.com
2. Settings → Access Tokens → Create Token

## CLI Commands

```bash
motherduck-sync <COMMAND>

Commands:
  sync     Sync data to MotherDuck
  test     Test connectivity
  status   Show unsynced record counts
  query    Query MotherDuck tables
  clean    Truncate or reset tables

Options:
  --full        Full sync (resync all)
  --json        JSON output
  -q, --quiet   Minimal output
```

### Examples

```bash
# Sync with progress
motherduck-sync sync

# Query synced data
motherduck-sync query --sql "SELECT COUNT(*) FROM full_users"

# Show all table counts
motherduck-sync query --counts

# Reset and resync
motherduck-sync clean --truncate
motherduck-sync sync --full
```

## GitHub Actions

The included workflow runs daily at 3 AM UTC:

```yaml
# .github/workflows/sync.yml
on:
  schedule:
    - cron: '0 3 * * *'
  workflow_dispatch:  # Manual trigger
```

### Setup

1. Go to your repo → Settings → Secrets → Actions
2. Add `DATABASE_URL` and `MOTHERDUCK_TOKEN`
3. The workflow runs automatically or via "Run workflow"

## Database Setup

### Supabase Views (Source)

Create views to transform your data:

```sql
-- Users view
CREATE VIEW analytics_full_users AS
SELECT
    id::text,
    nickname,
    email,
    created_time as created_at,
    updated_at
FROM profiles;

-- Listings view  
CREATE VIEW analytics_full_listings AS
SELECT
    id,
    profile_id::text,
    post_name,
    post_type,
    created_at,
    updated_at
FROM posts;
```

### Staging Tables (with sync tracking)

```sql
CREATE TABLE analytics_staging_users (
    id text PRIMARY KEY,
    nickname text,
    email text,
    created_at timestamptz,
    updated_at timestamptz,
    synced_to_motherduck boolean DEFAULT false
);

CREATE INDEX idx_staging_users_unsynced 
ON analytics_staging_users(synced_to_motherduck) 
WHERE NOT synced_to_motherduck;
```

## Synced Tables

| MotherDuck Table | Source Table | Description |
|------------------|--------------|-------------|
| `full_users` | `analytics_staging_users` | User profiles (email for analytics) |
| `full_listings` | `analytics_staging_listings` | Food listings with location |
| `daily_stats` | `analytics_daily_stats` | Daily platform aggregates |
| `user_activity_summary` | `analytics_user_activity` | Per-user activity metrics |
| `post_activity_daily_stats` | `analytics_post_activity` | Daily post activity |
| `full_rooms` | `analytics_staging_rooms` | Chat room metadata (no message content) |
| `full_reviews` | `analytics_staging_reviews` | User ratings and reviews |
| `full_favorites` | `analytics_staging_favorites` | Post bookmarks/favorites |

### Privacy Notes

- **No message content**: Chat rooms sync metadata only (message counts, timestamps)
- **No auth tokens**: Only profile data needed for analytics
- **Email included**: For user analytics; consider hashing if needed

## Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌─────────────┐
│   Supabase   │────▶│ motherduck-sync │────▶│ MotherDuck  │
│  PostgreSQL  │     │                 │     │   DuckDB    │
└──────────────┘     └─────────────────┘     └─────────────┘
       │                     │
       │                     ▼
       │             ┌───────────────┐
       └────────────▶│ Mark as synced│
                     └───────────────┘
```

1. Fetch unsynced records from Supabase
2. Batch insert to MotherDuck
3. Mark source records as synced

## Performance

- ~10,000 records/second throughput
- Batch size: 1,000 records
- Full sync of 12,865 records: ~19 seconds

## Troubleshooting

**Connection refused**
- Use Supabase pooler (port 6543), not direct connection (5432)

**SSL errors**
- Ensure `?sslmode=require` in connection string

**Slow sync**
- Add index on `synced_to_motherduck` column
- Increase batch size in config

## License

MIT OR Apache-2.0
