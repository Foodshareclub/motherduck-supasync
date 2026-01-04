# Troubleshooting

## Connection Issues

### "Connection refused"

**Cause:** Using direct PostgreSQL connection instead of pooler.

**Fix:** Use Supabase pooler (port 6543):
```
postgres://postgres.PROJECT:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres
```

Not direct connection (port 5432):
```
postgres://postgres.PROJECT:PASSWORD@db.PROJECT.supabase.co:5432/postgres
```

### "SSL required" / SSL errors

**Fix:** Add `?sslmode=require` to connection string:
```
postgres://...@host:6543/postgres?sslmode=require
```

### "Authentication failed"

**Causes:**
- Wrong password in DATABASE_URL
- User doesn't have permissions

**Fix:**
1. Verify password in Supabase Dashboard → Settings → Database
2. Check user has SELECT permissions on source tables

### "MotherDuck connection failed"

**Causes:**
- Invalid or expired token
- Network issues

**Fix:**
1. Generate new token at [app.motherduck.com](https://app.motherduck.com)
2. Update `MOTHERDUCK_TOKEN` secret

## Configuration Issues

### "No tables configured"

**Cause:** Missing table configuration.

**Fix:** Set one of:
```bash
# Option A: Base64 (CI/CD)
export SYNC_TABLES_CONFIG="$(cat tables.local.json | base64)"

# Option B: Plain JSON (local dev)
export SYNC_TABLES_JSON='[{"source":"table","target":"table","pk":["id"]}]'
```

### "Failed to decode SYNC_TABLES_CONFIG"

**Cause:** Invalid base64 encoding.

**Fix:** Regenerate the secret:
```bash
motherduck-sync generate-secret --input tables.local.json
```

**Verify:**
```bash
echo $SYNC_TABLES_CONFIG | base64 -d | jq .
```

### "Failed to parse table config JSON"

**Cause:** Invalid JSON syntax.

**Fix:** Validate your JSON:
```bash
cat tables.local.json | jq .
```

Common issues:
- Trailing commas
- Missing quotes
- Unescaped characters

### "Table not found"

**Cause:** Source table doesn't exist in PostgreSQL.

**Fix:**
1. Verify table name: `SELECT * FROM information_schema.tables WHERE table_name = 'your_table';`
2. Check schema (default is `public`)
3. Verify spelling in config

## Sync Issues

### "No records to sync"

**Causes:**
- All records already synced
- Filter excludes all records
- Wrong sync flag column

**Fix:**
1. Check unsynced count: `motherduck-sync status`
2. Verify sync flag: `SELECT COUNT(*) FROM table WHERE synced_to_motherduck = false;`
3. For full resync: `motherduck-sync sync --full`

### Slow sync performance

**Causes:**
- Missing index on sync flag
- Large batch size
- Network latency

**Fixes:**

1. Add partial index:
```sql
CREATE INDEX idx_table_unsynced 
ON your_table(synced_to_motherduck) 
WHERE NOT synced_to_motherduck;
```

2. Adjust batch size:
```toml
[sync]
batch_size = 500  # Smaller for complex rows
```

3. Increase pool size:
```toml
[postgres]
pool_size = 10
```

### "Column not found"

**Cause:** Column in config doesn't exist in source table.

**Fix:**
1. List columns: `SELECT column_name FROM information_schema.columns WHERE table_name = 'your_table';`
2. Update `columns` array in config

### Records not marked as synced

**Causes:**
- Using a view (can't update)
- Wrong sync flag column name
- Missing UPDATE permission

**Fix:**
1. Use staging tables instead of views
2. Verify column name matches config
3. Grant UPDATE permission

## Debug Mode

Enable verbose logging:

```bash
motherduck-sync --log-level debug sync
```

## Verify Configuration

```bash
# Check environment
echo "DATABASE_URL: ${DATABASE_URL:0:50}..."
echo "MOTHERDUCK_TOKEN: ${MOTHERDUCK_TOKEN:0:10}..."

# Decode and check table config
echo $SYNC_TABLES_CONFIG | base64 -d | jq .

# Test connectivity
motherduck-sync test

# Check unsynced counts
motherduck-sync status
```

## Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `connection refused` | Wrong port | Use port 6543 (pooler) |
| `SSL required` | Missing SSL mode | Add `?sslmode=require` |
| `authentication failed` | Wrong password | Check DATABASE_URL |
| `no tables configured` | Missing config | Set SYNC_TABLES_CONFIG |
| `table not found` | Wrong table name | Verify table exists |
| `column not found` | Wrong column name | Check column spelling |
| `permission denied` | Missing grants | Grant SELECT/UPDATE |

## Getting Help

1. Check logs with `--log-level debug`
2. Verify config with `echo $SYNC_TABLES_CONFIG | base64 -d`
3. Test connectivity with `motherduck-sync test`
4. Open an issue with error message and config (redact secrets!)
