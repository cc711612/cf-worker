# Cloudflare Worker Jobs API

Cloudflare Workers + D1 + Queues + Cron backend API.

## Current architecture

Request and execution flow:

1. Client calls `POST /jobs`.
2. Worker writes a job row into D1 (`status=pending`).
3. Worker sends `{ jobId }` to Cloudflare Queue `worker-queue`.
4. Queue consumer (`queue()` handler) claims the job (`pending -> processing`) and executes handler by `type`.
5. On success: update D1 to `done`.
6. On failure:
   - retryable error -> `attempts + 1`, compute backoff, set `next_run_at`, re-enqueue
   - non-retryable / max attempts reached -> `failed`

Resilience and fallback:

- `scheduled()` runs every minute.
- It recovers stale `processing` jobs (lock timeout).
- It re-enqueues due `pending` jobs as a safety net.

Core components:

- API + handlers: `src/index.ts`
- D1 schema: `migrations/0001_create_jobs.sql`
- Worker bindings (D1 + Queue + cron): `wrangler.toml`

## Features

- Hono HTTP API (`POST /jobs`, `GET /jobs/:id`, `POST /jobs/:id/retry`)
- Queue producer/consumer (`worker-queue`) for near real-time background processing
- D1-backed job state tracking (`jobs` table)
- Cron fallback for stale lock recovery and re-enqueue
- Idempotency via unique `(type, dedupe_key)`
- Payload masking in job lookup

## Setup

1. Install dependencies.
2. Create a D1 database.
3. Update `wrangler.toml` with your `database_id`.
4. Create queue `worker-queue` in Cloudflare (producer + consumer in same Worker).
5. Copy `.dev.vars.example` to `.dev.vars` and set `ADMIN_TOKEN`.
6. Run migration.

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

- For remote mode, ensure queue `worker-queue` exists in the same account.

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

## Job states

- `pending`: waiting for queue processing (or future `next_run_at`)
- `processing`: claimed by consumer
- `done`: completed successfully
- `failed`: exhausted retries or non-retryable error

## Retry policy

- Retries only for retryable errors.
- Backoff: `min(2^attempt * 30s, 1h) + jitter(0-10s)`.
- Default `max_attempts`: `5`.

## Queue, cron, and retries

- Queue: `worker-queue` receives `{ jobId }` messages
- Cron: every minute (`* * * * *`)
- Batch size: 20 jobs/run
- Retry delay: `min(2^attempt * 30s, 1h) + jitter(0-10s)`
- Stale lock recovery: 10 minutes, then re-enqueued

## Allowed job types

- `send_email`
- `sync_order`
- `cleanup`
