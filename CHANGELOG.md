# Changelog

## Unreleased

- fix: confine apalis's objects to the `apalis` schema (#86):
  - `generate_ulid` is now `apalis.generate_ulid` and no longer depends on `pgcrypto` — its random bytes come from core `gen_random_uuid()`. The sole caller (`apalis.push_job`) is repointed and the `public.generate_ulid` copy is dropped (via a new forward migration; existing migrations are not rewritten).
  - The sqlx migrations table is tracked in `apalis._sqlx_migrations` instead of `public._sqlx_migrations` (configured in a new `sqlx.toml`). This also isolates apalis's migration history from a user's own sqlx migrations on the same database, which previously collided over the shared default table name.
- bump: upgrade `sqlx` 0.8 → 0.9 (required for `sqlx.toml`); remap the runtime/TLS cargo features since 0.9 removed the combined `runtime-*-tls` flags.
- **Existing databases need a one-time manual step before upgrading** (fresh databases need nothing — `sqlx.toml` creates the `apalis` schema and tracking table). Move the migration history into the `apalis` schema and re-stamp the one edited migration's checksum — the first migration changed `CREATE SCHEMA` → `CREATE SCHEMA IF NOT EXISTS` so the `apalis` schema can be created before the tracking table on fresh installs. See "Upgrading to 1.0" in the README for the SQL; it applies to every apply path (`setup()`, sqlx-cli, copied migrations, or a merged `Migrator`).
- note: `pgcrypto` is no longer used by apalis but is left where an earlier version installed it (usually `public`). If nothing else needs it, you can `DROP EXTENSION pgcrypto;`.

## [1.0.0-rc.8] - 2026-05-08

- feat: idempotency for tasks (#81)
- chore: make JsonCodec publicly accessible (#79)

## [1.0.0-rc.7] - 2026-04-09

- bump: introducing rc.7

## [1.0.0-rc.6] - 2026-03-12

- bump: introducing rc.6

## [1.0.0-rc.4] - 2026-02-21

- bump: introducing rc.4

## [1.0.0-rc.3] - 2026-02-02

- bump: introducing rc.3

## [1.0.0-rc.2] - 2026-01-10

- bump: introducing rc.2
- feat: use DateTime abstraction from apalis-sql
- bump: update apalis deps to rc.2

## [1.0.0-rc.1] - 2025-12-27

- bump: introducing rc.1 (#45)
- fix: Add primary keys for database tables (#45)

## [1.0.0-beta.3] - 2025-12-06

- fix: correct allowSelfAssign param as bool (#25) 
- fix: ensure automated release (#27)
- fix: ensure workflow_call (#28)


## [0.7.1] - 2025-03-17

### 🐛 Bug Fixes

- Reenqueue oprphaned before starting streaming (#507)
- PostgresStorage get_jobs status conditional (#524)

### 💼 Other

- Generic retry persist check (#498)
- Add associated types to the `Backend` trait (#516)
## [0.6.4] - 2024-12-03

### 🐛 Bug Fixes

- Allow polling only when worker is ready (#472)
## [0.5.5] - 2024-05-20

### 🐛 Bug Fixes

- Wrong timestamp type for pg (#321)
## [0.4.9] - 2024-01-03

### 🚀 Features

- Configurable worker set as dead (#220)
## [0.4.7] - 2023-11-15

### 🐛 Bug Fixes

- Allow cargo build --all-features (#204)
## [0.4.5] - 2023-10-08

### 💼 Other

- Api to get migrations
## [0.4.4] - 2023-07-31

### 🐛 Bug Fixes

- Change approach for mysql

### 💼 Other

- Sqlx to v0.7
## [0.3.0] - 2022-06-05
