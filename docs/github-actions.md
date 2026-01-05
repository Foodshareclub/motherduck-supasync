# GitHub Actions

## Overview

The included workflow automates syncing on a schedule or manual trigger.

## Setup

### 1. Add Secrets

Go to your repository: **Settings → Secrets and variables → Actions**

| Secret | Description |
|--------|-------------|
| `DATABASE_URL` | Supabase pooler connection string |
| `MOTHERDUCK_TOKEN` | MotherDuck access token |
| `SYNC_TABLES_CONFIG` | Base64-encoded table config |

### 2. Generate SYNC_TABLES_CONFIG

```bash
# Create your config
cp tables.example.json tables.local.json
# Edit tables.local.json with your tables

# Generate the secret
motherduck-supasync generate-secret --input tables.local.json
```

Copy the output and paste as `SYNC_TABLES_CONFIG` secret value.

## Workflow Configuration

### Default Schedule

```yaml
on:
  schedule:
    - cron: '0 3 * * *'   # 3 AM UTC
    - cron: '0 11 * * *'  # 11 AM UTC
    - cron: '0 19 * * *'  # 7 PM UTC
  workflow_dispatch:       # Manual trigger
```

### Manual Trigger Options

When triggering manually via GitHub UI:

| Input | Options | Default |
|-------|---------|---------|
| Sync mode | `incremental`, `full` | `incremental` |
| Log level | `info`, `debug`, `warn` | `info` |

## Workflow File

Location: `.github/workflows/sync.yml`

```yaml
name: Sync to MotherDuck

on:
  schedule:
    - cron: '0 3 * * *'
    - cron: '0 11 * * *'
    - cron: '0 19 * * *'
  workflow_dispatch:
    inputs:
      sync_mode:
        description: 'Sync mode'
        required: false
        default: 'incremental'
        type: choice
        options:
          - incremental
          - full
      log_level:
        description: 'Log level'
        required: false
        default: 'info'
        type: choice
        options:
          - info
          - debug
          - warn

concurrency:
  group: motherduck-supasync
  cancel-in-progress: false

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  sync:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Rust
        uses: dtolnay/rust-action@stable
        
      - name: Cache cargo
        uses: actions/cache@v4
        with:
          path: |
            ~/.cargo/bin/
            ~/.cargo/registry/
            ~/.cargo/git/
            target/
          key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
          
      - name: Build
        run: cargo build --release
        
      - name: Test connectivity
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
          SYNC_TABLES_CONFIG: ${{ secrets.SYNC_TABLES_CONFIG }}
        run: ./target/release/motherduck-supasync test
        
      - name: Run sync
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
          SYNC_TABLES_CONFIG: ${{ secrets.SYNC_TABLES_CONFIG }}
          LOG_LEVEL: ${{ inputs.log_level || 'info' }}
        run: |
          if [ "${{ inputs.sync_mode }}" = "full" ]; then
            ./target/release/motherduck-supasync sync --full
          else
            ./target/release/motherduck-supasync sync
          fi
```

## Security Notes

- Table names are redacted in logs
- Secrets never appear in output
- Use `SYNC_TABLES_CONFIG` to keep schema private

## Monitoring

### Check Workflow Runs

1. Go to **Actions** tab in your repository
2. Select "Sync to MotherDuck" workflow
3. View run history and logs

### Notifications

Add Slack/Discord notifications:

```yaml
- name: Notify on failure
  if: failure()
  uses: 8398a7/action-slack@v3
  with:
    status: failure
    fields: repo,message,commit,author
  env:
    SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK }}
```
