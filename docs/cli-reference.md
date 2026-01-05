# CLI Reference

## Synopsis

```
motherduck-supasync [OPTIONS] <COMMAND>
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
motherduck-supasync sync

# Full sync (all records)
motherduck-supasync sync --full

# With custom config
motherduck-supasync --config my-config.toml sync
```

### test

Test connectivity to PostgreSQL and MotherDuck.

```bash
motherduck-supasync test

# JSON output
motherduck-supasync test --json
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
motherduck-supasync status

# JSON output
motherduck-supasync status --json
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
motherduck-supasync query --tables

# Show row counts
motherduck-supasync query --counts

# Execute custom SQL
motherduck-supasync query --sql "SELECT COUNT(*) FROM full_users"
motherduck-supasync query --sql "SELECT * FROM daily_stats LIMIT 5"
```

### clean

Clean/reset MotherDuck tables.

```bash
# Truncate all tables (keep structure)
motherduck-supasync clean --truncate

# Truncate specific table
motherduck-supasync clean --truncate -t daily_stats

# Drop and recreate tables
motherduck-supasync clean --reset
```

### init

Generate a sample TOML configuration file.

```bash
motherduck-supasync init
motherduck-supasync init --output my-config.toml
```

### generate-secret

Generate base64-encoded secret from JSON file.

```bash
# Default: tables.local.json
motherduck-supasync generate-secret

# Custom input file
motherduck-supasync generate-secret --input my-tables.json
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
motherduck-supasync test

# 2. Check what needs syncing
motherduck-supasync status

# 3. Run incremental sync
motherduck-supasync sync

# 4. Verify results
motherduck-supasync query --counts
```

### Full Resync

```bash
# Clear existing data
motherduck-supasync clean --truncate

# Resync everything
motherduck-supasync sync --full
```

### Debug Mode

```bash
# Verbose logging
motherduck-supasync --log-level debug sync

# JSON output for parsing
motherduck-supasync --json status
```

### CI/CD Usage

```bash
# Quiet mode for CI
motherduck-supasync -q sync

# JSON output for parsing
motherduck-supasync --json sync
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (check stderr) |
