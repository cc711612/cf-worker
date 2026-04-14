# Cloudflare Worker Jobs API

Cloudflare Workers + D1 + Cron backend API without Cloudflare Queues.

## Features

- Hono HTTP API (`POST /jobs`, `GET /jobs/:id`, `POST /jobs/:id/retry`)
- D1-backed pseudo queue (`jobs` table)
- Cron processing with stale lock recovery and retries
- Idempotency via unique `(type, dedupe_key)`
- Payload masking in job lookup

## Setup

1. Install dependencies.
2. Create a D1 database.
3. Update `wrangler.toml` with your `database_id`.
4. Copy `.dev.vars.example` to `.dev.vars` and set `ADMIN_TOKEN`.
5. Run migration.

## Commands

```bash
npm install
npx wrangler d1 migrations apply cf-worker-db --local
npm run dev
```

## Local vs remote test mode

- `npm run dev` / `npm run dev:remote`: run Worker in remote mode and use remote bindings.
- `npm run dev:local`: run Worker with local bindings/state.
- If you use remote mode, apply migration to remote first:

```bash
npx wrangler d1 migrations apply cf-worker-db --remote
```

## API examples

Create job:

```bash
curl -X POST http://127.0.0.1:8787/jobs \
  -H "content-type: application/json" \
  -d '{"type":"send_email","payload":{"to":"demo@example.com","subject":"hi"},"dedupe_key":"email-demo-1"}'
```

Get job:

```bash
curl http://127.0.0.1:8787/jobs/<job_id>
```

Retry failed job:

```bash
curl -X POST http://127.0.0.1:8787/jobs/<job_id>/retry \
  -H "x-admin-token: replace-me"
```

## Cron and retries

- Cron: every minute (`* * * * *`)
- Batch size: 20 jobs/run
- Retry delay: `min(2^attempt * 30s, 1h) + jitter(0-10s)`
- Stale lock recovery: 10 minutes

## Allowed job types

- `send_email`
- `sync_order`
- `cleanup`
