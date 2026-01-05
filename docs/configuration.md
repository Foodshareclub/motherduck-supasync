# Configuration

MotherDuck Sync supports three configuration methods:
1. **Environment variables** (recommended for CI/CD)
2. **JSON file** (for table mappings)
3. **TOML file** (for complex setups)

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string (Supabase pooler) |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |

### Table Configuration (one required)

| Variable | Description |
|----------|-------------|
| `SYNC_TABLES_CONFIG` | Base64-encoded JSON table config (for CI/CD) |
| `SYNC_TABLES_JSON` | Plain JSON table config (for local dev) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `MOTHERDUCK_DATABASE` | `analytics` | Target database name |
| `LOG_LEVEL` | `info` | Logging level |

## Table Configuration (JSON)

### Creating tables.local.json

1. Copy the example file:
   ```bash
   cp tables.example.json tables.local.json
   ```

2. Edit with your actual table names:
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

3. Keep `tables.local.json` gitignored (it contains your schema)

### Generating the Secret

Use the built-in command:

```bash
motherduck-supasync generate-secret --input tables.local.json
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

**Manual alternative:**
```bash
cat tables.local.json | jq -c | base64
```

### Field Reference

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `source` | ✅ | - | Source table name in PostgreSQL |
| `target` | ✅ | - | Target table name in MotherDuck |
| `pk` | ✅ | - | Primary key column(s) as array |
| `columns` | ❌ | all | Columns to sync (empty = all) |
| `mappings` | ❌ | {} | Column renames: `{"source": "target"}` |
| `order_by` | ❌ | null | ORDER BY column |
| `filter` | ❌ | null | WHERE clause (without WHERE) |
| `enabled` | ❌ | true | Enable/disable this table |

### Examples

**Basic:**
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
  "filter": "status = 'completed'"
}
```

**Composite primary key:**
```json
{"source": "user_roles", "target": "user_roles", "pk": ["user_id", "role_id"]}
```

**Disabled:**
```json
{"source": "legacy", "target": "legacy", "pk": ["id"], "enabled": false}
```

## TOML Configuration (Alternative)

For complex setups, generate a TOML config:

```bash
motherduck-supasync init --output motherduck-supasync.toml
```

### Full TOML Reference

```toml
[postgres]
url = "postgres://user:password@host:6543/database?sslmode=require"
pool_size = 5                    # Connection pool size (1-100)
connect_timeout_secs = 30        # Connection timeout

[motherduck]
token = "your_token"             # Or use MOTHERDUCK_TOKEN env var
database = "analytics"           # Target database
schema = "main"                  # Target schema
create_database = true           # Auto-create if missing

[sync]
batch_size = 1000                # Records per batch (1-100000)
use_transactions = true          # Wrap in transactions
mark_synced = true               # Update sync flag after sync
sync_flag_column = "synced_to_motherduck"  # Column name for flag
auto_create_tables = true        # Create target tables
max_records = 0                  # Limit per sync (0 = unlimited)

[retry]
max_retries = 3                  # Retry attempts (0-10)
initial_backoff_ms = 1000        # Initial retry delay
max_backoff_ms = 60000           # Max retry delay
multiplier = 2.0                 # Backoff multiplier
jitter = true                    # Add randomness to backoff

[logging]
level = "info"                   # debug, info, warn, error
format = "text"                  # text or json
timestamps = true

[[tables]]
source_table = "analytics_staging_users"
target_table = "full_users"
primary_key = ["id"]
sync_flag_column = "synced_to_motherduck"
columns = ["id", "nickname", "email", "created_at"]
order_by = "created_at"
enabled = true

[[tables]]
source_table = "analytics_daily_stats"
target_table = "daily_stats"
primary_key = ["date"]
enabled = true
```

### Using TOML Config

```bash
motherduck-supasync --config motherduck-supasync.toml sync
```

## Configuration Priority

1. Command-line arguments (highest)
2. Environment variables
3. TOML config file
4. Defaults (lowest)
