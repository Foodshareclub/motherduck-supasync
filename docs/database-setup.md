# Database Setup

## Overview

MotherDuck Sync requires:
1. **Source tables** in PostgreSQL with a sync tracking column
2. **Target tables** in MotherDuck (auto-created or manual)

## PostgreSQL (Supabase) Setup

### Option 1: Staging Tables (Recommended)

Create dedicated staging tables with sync tracking:

```sql
-- Users staging table
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

### Option 2: Views (Read-Only)

For read-only sync without marking:

```sql
CREATE VIEW analytics_staging_users AS
SELECT
    id::text,
    nickname,
    email,
    created_time as created_at,
    updated_at,
    false as synced_to_motherduck
FROM profiles;
```

> Note: Views don't support marking records as synced. Use `--full` sync mode.

### Populating Staging Tables

**Trigger-based (real-time):**

```sql
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

CREATE TRIGGER profiles_to_staging
AFTER INSERT OR UPDATE ON profiles
FOR EACH ROW EXECUTE FUNCTION sync_to_staging();
```

**Batch update (scheduled):**

```sql
-- Run periodically via pg_cron or external scheduler
INSERT INTO analytics_staging_users (id, nickname, email, created_at, updated_at)
SELECT id::text, nickname, email, created_time, updated_at
FROM profiles
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM analytics_staging_users)
ON CONFLICT (id) DO UPDATE SET
    nickname = EXCLUDED.nickname,
    email = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at,
    synced_to_motherduck = false;
```

### Example Tables

**Daily Stats:**
```sql
CREATE TABLE analytics_daily_stats (
    date date PRIMARY KEY,
    total_users integer DEFAULT 0,
    active_users integer DEFAULT 0,
    new_listings integer DEFAULT 0,
    completed_pickups integer DEFAULT 0,
    synced_to_motherduck boolean DEFAULT false
);

CREATE INDEX idx_daily_stats_unsynced 
ON analytics_daily_stats(synced_to_motherduck) 
WHERE NOT synced_to_motherduck;
```

**User Activity:**
```sql
CREATE TABLE analytics_user_activity (
    user_id text PRIMARY KEY,
    total_listings integer DEFAULT 0,
    total_pickups integer DEFAULT 0,
    last_active_at timestamptz,
    synced_to_motherduck boolean DEFAULT false
);
```

**Post Activity:**
```sql
CREATE TABLE analytics_post_activity (
    id serial PRIMARY KEY,
    post_id integer NOT NULL,
    date date NOT NULL,
    views integer DEFAULT 0,
    likes integer DEFAULT 0,
    synced_to_motherduck boolean DEFAULT false,
    UNIQUE(post_id, date)
);
```

## MotherDuck Setup

### Auto-Creation

Tables are automatically created on first sync based on source schema.

### Manual Creation

```sql
-- Connect to MotherDuck
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- Users table
CREATE TABLE full_users (
    id VARCHAR PRIMARY KEY,
    nickname VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Daily stats
CREATE TABLE daily_stats (
    date DATE PRIMARY KEY,
    total_users INTEGER,
    active_users INTEGER,
    new_listings INTEGER,
    completed_pickups INTEGER
);

-- User activity
CREATE TABLE user_activity_summary (
    user_id VARCHAR PRIMARY KEY,
    total_listings INTEGER,
    total_pickups INTEGER,
    last_active_at TIMESTAMP
);
```

## Connection Strings

### Supabase Pooler (Recommended)

```
postgres://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres?sslmode=require
```

- Use port **6543** (pooler), not 5432 (direct)
- Always include `?sslmode=require`
- Replace `PROJECT_REF` with your project reference
- Replace `PASSWORD` with your database password
- Replace `REGION` with your region (e.g., `us-east-1`)

### Finding Your Connection String

1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project
3. Settings → Database
4. Copy "Connection string" under **Connection pooling**

## Performance Tips

### Indexes

Always add partial indexes on the sync flag:

```sql
CREATE INDEX idx_table_unsynced 
ON your_table(synced_to_motherduck) 
WHERE NOT synced_to_motherduck;
```

### Batch Size

Default batch size is 1000. Adjust based on your data:

```toml
[sync]
batch_size = 5000  # Larger for simple rows
```

### Connection Pool

Default pool size is 5. Increase for high-volume syncs:

```toml
[postgres]
pool_size = 10
```
