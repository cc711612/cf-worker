import { Context, Hono } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { z } from "zod";

type JobStatus = "pending" | "processing" | "done" | "failed";

const JOB_TYPES = ["send_email", "sync_order", "cleanup"] as const;
type JobType = (typeof JOB_TYPES)[number];

type JobMessage = {
  jobId: string;
};

type Env = {
  DB: D1Database;
  JOB_QUEUE: Queue<JobMessage>;
  ADMIN_TOKEN: string;
};

type JobRow = {
  id: string;
  type: JobType;
  payload: string;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  next_run_at: string;
  dedupe_key: string | null;
  locked_at: string | null;
  last_error: string | null;
  created_at: string;
  updated_at: string;
};

const STALE_LOCK_MINUTES = 10;
const CRON_BATCH_SIZE = 20;
const MAX_QUEUE_DELAY_SECONDS = 12 * 60 * 60;
const SENSITIVE_KEYS = ["email", "token", "phone", "password", "secret", "authorization"];

const createJobSchema = z.object({
  type: z.enum(JOB_TYPES),
  payload: z.record(z.string(), z.unknown()),
  dedupe_key: z.string().min(1).max(128).optional(),
  max_attempts: z.number().int().min(1).max(10).optional(),
  run_at: z.string().datetime().optional()
});

const app = new Hono<{ Bindings: Env }>();

app.get("/health", (c) => c.json({ ok: true }));

app.post("/jobs", async (c) => {
  const parsed = createJobSchema.safeParse(await c.req.json().catch(() => null));
  if (!parsed.success) {
    return jsonError(c, 400, "VALIDATION_ERROR", "Invalid request body", parsed.error.flatten());
  }

  const body = parsed.data;
  const id = crypto.randomUUID();
  const maxAttempts = body.max_attempts ?? 5;
  const initialRunAt = body.run_at ? new Date(body.run_at) : new Date();

  try {
    await c.env.DB.prepare(
      `
      INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, next_run_at, dedupe_key)
      VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
      `
    )
      .bind(
        id,
        body.type,
        JSON.stringify(body.payload),
        maxAttempts,
        toSqliteDate(initialRunAt),
        body.dedupe_key ?? null
      )
      .run();
  } catch (error) {
    if (isUniqueConstraintError(error) && body.dedupe_key) {
      const existing = await c.env.DB.prepare(
        `
        SELECT id, type, status, attempts, max_attempts, next_run_at, created_at
        FROM jobs
        WHERE type = ? AND dedupe_key = ?
        LIMIT 1
        `
      )
        .bind(body.type, body.dedupe_key)
        .first<{
          id: string;
          type: JobType;
          status: JobStatus;
          attempts: number;
          max_attempts: number;
          next_run_at: string;
          created_at: string;
        }>();

      if (!existing) {
        return jsonError(c, 409, "CONFLICT", "Job already exists but could not be fetched");
      }

      return c.json({ ...existing, deduped: true }, 200);
    }

    return jsonError(c, 500, "INTERNAL_ERROR", "Failed to create job");
  }

  const delaySeconds = getInitialQueueDelaySeconds(initialRunAt);
  if (delaySeconds !== null) {
    await enqueueJob(c.env, id, delaySeconds);
  }

  const created = await c.env.DB.prepare(
    `
    SELECT id, type, status, attempts, max_attempts, next_run_at, created_at
    FROM jobs
    WHERE id = ?
    LIMIT 1
    `
  )
    .bind(id)
    .first<{
      id: string;
      type: JobType;
      status: JobStatus;
      attempts: number;
      max_attempts: number;
      next_run_at: string;
      created_at: string;
    }>();

  if (!created) {
    return jsonError(c, 500, "INTERNAL_ERROR", "Job was created but not found");
  }

  return c.json({ ...created, deduped: false }, 201);
});

app.get("/jobs/:id", async (c) => {
  const id = c.req.param("id");

  const job = await c.env.DB.prepare(
    `
    SELECT id, type, payload, status, attempts, max_attempts, next_run_at, last_error, created_at, updated_at
    FROM jobs
    WHERE id = ?
    LIMIT 1
    `
  )
    .bind(id)
    .first<JobRow>();

  if (!job) {
    return jsonError(c, 404, "NOT_FOUND", "Job not found");
  }

  const safePayload = maskPayload(parsePayload(job.payload));

  return c.json({
    id: job.id,
    type: job.type,
    payload: safePayload,
    status: job.status,
    attempts: job.attempts,
    max_attempts: job.max_attempts,
    next_run_at: job.next_run_at,
    last_error: job.last_error,
    created_at: job.created_at,
    updated_at: job.updated_at
  });
});

app.post("/jobs/:id/retry", async (c) => {
  if (c.req.header("x-admin-token") !== c.env.ADMIN_TOKEN) {
    return jsonError(c, 401, "UNAUTHORIZED", "Invalid admin token");
  }

  const id = c.req.param("id");
  const result = await c.env.DB.prepare(
    `
    UPDATE jobs
    SET status = 'pending',
        next_run_at = CURRENT_TIMESTAMP,
        locked_at = NULL,
        last_error = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
      AND (
        status = 'failed'
        OR (
          status = 'processing'
          AND locked_at < datetime(CURRENT_TIMESTAMP, ?)
        )
      )
    `
  )
    .bind(id, `-${STALE_LOCK_MINUTES} minutes`)
    .run();

  if (!result.meta.changes) {
    return jsonError(c, 409, "CONFLICT", "Job is not retryable in current state");
  }

  await enqueueJob(c.env, id, 0);

  const job = await c.env.DB.prepare(
    `
    SELECT id, type, status, attempts, max_attempts, next_run_at, created_at, updated_at
    FROM jobs
    WHERE id = ?
    LIMIT 1
    `
  )
    .bind(id)
    .first<{
      id: string;
      type: JobType;
      status: JobStatus;
      attempts: number;
      max_attempts: number;
      next_run_at: string;
      created_at: string;
      updated_at: string;
    }>();

  return c.json(job, 200);
});

async function handleScheduled(env: Env): Promise<void> {
  await recoverStaleJobs(env);
  await enqueueDueJobs(env, CRON_BATCH_SIZE);
}

async function handleQueue(batch: MessageBatch<JobMessage>, env: Env): Promise<void> {
  for (const message of batch.messages) {
    const jobId = message.body?.jobId;
    if (!jobId) {
      continue;
    }

    await processQueuedJob(env, jobId);
  }
}

async function processQueuedJob(env: Env, jobId: string): Promise<void> {
  const claimed = await claimJobForProcessing(env, jobId);
  if (!claimed) {
    return;
  }

  try {
    const payload = parsePayload(claimed.payload);
    await executeJob(claimed.type, payload, claimed.id);

    await env.DB.prepare(
      `
      UPDATE jobs
      SET status = 'done',
          locked_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
      `
    )
      .bind(claimed.id)
      .run();
  } catch (error) {
    const retryable = isRetryableError(error);
    const nextAttempt = claimed.attempts + 1;
    const errorMessage = extractErrorMessage(error);

    if (retryable && nextAttempt < claimed.max_attempts) {
      const { nextRunAt, delaySeconds } = buildRetrySchedule(nextAttempt);

      await env.DB.prepare(
        `
        UPDATE jobs
        SET status = 'pending',
            attempts = attempts + 1,
            next_run_at = ?,
            last_error = ?,
            locked_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        `
      )
        .bind(nextRunAt, errorMessage, claimed.id)
        .run();

      await enqueueJob(env, claimed.id, delaySeconds);
      return;
    }

    await env.DB.prepare(
      `
      UPDATE jobs
      SET status = 'failed',
          attempts = attempts + 1,
          last_error = ?,
          locked_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
      `
    )
      .bind(errorMessage, claimed.id)
      .run();
  }
}

async function claimJobForProcessing(env: Env, jobId: string): Promise<JobRow | null> {
  const claim = await env.DB.prepare(
    `
    UPDATE jobs
    SET status = 'processing',
        locked_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
      AND status = 'pending'
      AND datetime(next_run_at) <= CURRENT_TIMESTAMP
    `
  )
    .bind(jobId)
    .run();

  if (!claim.meta.changes) {
    return null;
  }

  return env.DB.prepare(
    `
    SELECT id, type, payload, status, attempts, max_attempts, next_run_at, dedupe_key, locked_at, last_error, created_at, updated_at
    FROM jobs
    WHERE id = ?
    LIMIT 1
    `
  )
    .bind(jobId)
    .first<JobRow>();
}

async function recoverStaleJobs(env: Env): Promise<void> {
  await env.DB.prepare(
    `
    UPDATE jobs
    SET status = 'pending',
        next_run_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP,
        last_error = COALESCE(last_error, 'Recovered from stale lock'),
        locked_at = NULL
    WHERE status = 'processing'
      AND locked_at < datetime(CURRENT_TIMESTAMP, ?)
    `
  )
    .bind(`-${STALE_LOCK_MINUTES} minutes`)
    .run();
}

async function enqueueDueJobs(env: Env, batchSize: number): Promise<void> {
  const result = await env.DB.prepare(
    `
    SELECT id
    FROM jobs
    WHERE status = 'pending'
      AND datetime(next_run_at) <= CURRENT_TIMESTAMP
    ORDER BY datetime(next_run_at) ASC
    LIMIT ?
    `
  )
    .bind(batchSize)
    .all<{ id: string }>();

  for (const row of result.results) {
    await enqueueJob(env, row.id, 0);
  }
}

async function enqueueJob(env: Env, jobId: string, delaySeconds: number): Promise<void> {
  if (delaySeconds > 0) {
    await env.JOB_QUEUE.send({ jobId }, { delaySeconds: Math.min(delaySeconds, MAX_QUEUE_DELAY_SECONDS) });
    return;
  }

  await env.JOB_QUEUE.send({ jobId });
}

function getInitialQueueDelaySeconds(runAt: Date): number | null {
  const seconds = Math.ceil((runAt.getTime() - Date.now()) / 1000);
  if (seconds <= 0) {
    return 0;
  }

  if (seconds > MAX_QUEUE_DELAY_SECONDS) {
    return null;
  }

  return seconds;
}

async function executeJob(type: JobType, payload: Record<string, unknown>, jobId: string): Promise<void> {
  switch (type) {
    case "send_email": {
      if (typeof payload.to !== "string" || !payload.to) {
        throw new JobError("Missing email recipient", false);
      }

      console.log("send_email", { jobId, to: payload.to });
      return;
    }
    case "sync_order": {
      if (typeof payload.order_id !== "string" || !payload.order_id) {
        throw new JobError("Missing order_id", false);
      }

      console.log("sync_order", { jobId, orderId: payload.order_id });
      return;
    }
    case "cleanup": {
      console.log("cleanup", { jobId });
      return;
    }
    default:
      throw new JobError("Unsupported job type", false);
  }
}

function buildRetrySchedule(nextAttempt: number): { nextRunAt: string; delaySeconds: number } {
  const delaySeconds = Math.min(2 ** nextAttempt * 30, 3600) + randomInt(0, 10);
  const nextRunAt = new Date(Date.now() + delaySeconds * 1000);

  return {
    nextRunAt: toSqliteDate(nextRunAt),
    delaySeconds
  };
}

function toSqliteDate(date: Date): string {
  return date.toISOString().slice(0, 19).replace("T", " ");
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function parsePayload(payloadText: string): Record<string, unknown> {
  const parsed = JSON.parse(payloadText);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new JobError("Payload must be a JSON object", false);
  }

  return parsed as Record<string, unknown>;
}

function isSensitiveKey(key: string): boolean {
  const lower = key.toLowerCase();
  return SENSITIVE_KEYS.some((target) => lower.includes(target));
}

function maskPayload(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(maskPayload);
  }

  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).map(([key, item]) => {
      if (isSensitiveKey(key)) {
        return [key, "***"];
      }

      return [key, maskPayload(item)];
    });

    return Object.fromEntries(entries);
  }

  return value;
}

function isUniqueConstraintError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  return error.message.includes("UNIQUE constraint failed");
}

function isRetryableError(error: unknown): boolean {
  if (error instanceof JobError) {
    return error.retryable;
  }

  return false;
}

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message.slice(0, 1000);
  }

  return "Unknown error";
}

function jsonError(
  c: Context,
  status: ContentfulStatusCode,
  code: string,
  message: string,
  details?: unknown
) {
  return c.json({ code, message, details }, status);
}

class JobError extends Error {
  retryable: boolean;

  constructor(message: string, retryable: boolean) {
    super(message);
    this.retryable = retryable;
  }
}

export default {
  fetch: app.fetch,
  queue(batch: MessageBatch<JobMessage>, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(handleQueue(batch, env));
  },
  scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(handleScheduled(env));
  }
};
