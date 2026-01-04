# Getting Started

## Prerequisites

- Rust 1.85+ (for building from source)
- PostgreSQL database (Supabase recommended)
- MotherDuck account

## Installation

### From Source

```bash
git clone https://github.com/Foodshareclub/motherduck-sync
cd motherduck-sync
cargo install --path .
```

### Build Only

```bash
cargo build --release
# Binary at: target/release/motherduck-sync
```

## Quick Setup

### 1. Get Your Credentials

**Supabase DATABASE_URL:**
1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project → Settings → Database
3. Copy "Connection string" under **Connection pooling** (port 6543)
4. Replace `[YOUR-PASSWORD]` with your database password

> ⚠️ Use the **pooler** connection (port 6543), not direct (5432)

**MotherDuck Token:**
1. Go to [app.motherduck.com](https://app.motherduck.com)
2. Settings → Access Tokens → Create Token

### 2. Set Environment Variables

```bash
export DATABASE_URL="postgres://postgres.PROJECT:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres?sslmode=require"
export MOTHERDUCK_TOKEN="your_token_here"
```

### 3. Create Table Configuration

```bash
# Copy the example
cp tables.example.json tables.local.json

# Edit with your tables
```

Example `tables.local.json`:
```json
[
  {
    "source": "analytics_staging_users",
    "target": "full_users",
    "pk": ["id"],
    "columns": ["id", "nickname", "email", "created_at"],
    "order_by": "created_at"
  }
]
```

### 4. Set Table Config

```bash
# Option A: Plain JSON (local dev)
export SYNC_TABLES_JSON='[{"source":"my_table","target":"target","pk":["id"]}]'

# Option B: Base64 encoded (production/CI)
export SYNC_TABLES_CONFIG="$(cat tables.local.json | base64)"
```

### 5. Test and Sync

```bash
# Test connectivity
motherduck-sync test

# Check what needs syncing
motherduck-sync status

# Run sync
motherduck-sync sync
```

## Next Steps

- [Configuration](./configuration.md) - All configuration options
- [Database Setup](./database-setup.md) - Set up source tables
- [GitHub Actions](./github-actions.md) - Automate syncing
