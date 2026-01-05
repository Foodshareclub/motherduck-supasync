# MotherDuck Sync Documentation

## Quick Links

| Document | Description |
|----------|-------------|
| [Getting Started](./getting-started.md) | Installation and first sync |
| [Configuration](./configuration.md) | All configuration options |
| [CLI Reference](./cli-reference.md) | Command-line usage |
| [Database Setup](./database-setup.md) | PostgreSQL and MotherDuck setup |
| [GitHub Actions](./github-actions.md) | CI/CD automation |
| [Troubleshooting](./troubleshooting.md) | Common issues and solutions |

## Overview

MotherDuck Sync is a Rust CLI for syncing data from PostgreSQL (Supabase) to MotherDuck for analytics.

```
PostgreSQL/Supabase  →  motherduck-supasync  →  MotherDuck (DuckDB)
     (source)              (this tool)         (analytics)
```

## Features

- Incremental and full sync modes
- Batch processing (~10k records/sec)
- Column mapping and filtering
- Privacy-first (schema in secrets)
- GitHub Actions ready
