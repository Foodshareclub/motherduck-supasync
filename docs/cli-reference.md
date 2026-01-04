# CLI Reference

## Synopsis

```
motherduck-sync [OPTIONS] <COMMAND>
```

## Global Options

| Option | Description |
|--------|-------------|
| `--full` | Full sync (resync all records) |
| `-c, --config <FILE>` | Config file path |
| `--log-level <LEVEL>` | Log level: debug, info, warn, error |
| `--json` | JSON output format |
| `-q, --quiet` | Minimal output |
| `-h, --help` | Print help |
| `-V, --version` | Print version |

## Commands

### sync

Run data synchronization (default command).

```bash
# Incremental sync (only unsynced records)
motherduck-sync sync

# Full sync (all records)
motherduck-sync sync --full

# With custom config
motherduck-sync --config my-config.toml sync
```

### test

Test connectivity to PostgreSQL and MotherDuck.

```bash
motherduck-sync test

# JSON output
motherduck-sync test --json
```

Output:
```
Testing connectivity...

✓ PostgreSQL: Connected
✓ MotherDuck: Connected

✓ All connectivity tests passed!
```

### status

Show count of unsynced records per table.

```bash
motherduck-sync status

# JSON output
motherduck-sync status --json
```

Output:
```
Unsynced Records

  analytics_staging_users: 150 records
  analytics_daily_stats: 7 records

Total: 157 unsynced
```

### query

Query MotherDuck tables directly.

```bash
# List all tables
motherduck-sync query --tables

# Show row counts
motherduck-sync query --counts

# Execute custom SQL
motherduck-sync query --sql "SELECT COUNT(*) FROM full_users"
motherduck-sync query --sql "SELECT * FROM daily_stats LIMIT 5"
```

### clean

Clean/reset MotherDuck tables.

```bash
# Truncate all tables (keep structure)
motherduck-sync clean --truncate

# Truncate specific table
motherduck-sync clean --truncate -t daily_stats

# Drop and recreate tables
motherduck-sync clean --reset
```

### init

Generate a sample TOML configuration file.

```bash
motherduck-sync init
motherduck-sync init --output my-config.toml
```

### generate-secret

Generate base64-encoded secret from JSON file.

```bash
# Default: tables.local.json
motherduck-sync generate-secret

# Custom input file
motherduck-sync generate-secret --input my-tables.json
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

## Examples

### Basic Workflow

```bash
# 1. Test connectivity
motherduck-sync test

# 2. Check what needs syncing
motherduck-sync status

# 3. Run incremental sync
motherduck-sync sync

# 4. Verify results
motherduck-sync query --counts
```

### Full Resync

```bash
# Clear existing data
motherduck-sync clean --truncate

# Resync everything
motherduck-sync sync --full
```

### Debug Mode

```bash
# Verbose logging
motherduck-sync --log-level debug sync

# JSON output for parsing
motherduck-sync --json status
```

### CI/CD Usage

```bash
# Quiet mode for CI
motherduck-sync -q sync

# JSON output for parsing
motherduck-sync --json sync
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (check stderr) |
