# EcoGuard Uganda — backend

FastAPI service: cookie authentication + X-CSRF-Token, private evidence,
human-reviewed advisories, Redis cache and a realtime SSE stream
(`GET /api/v1/stream`). Runs on PostgreSQL, deployed on Vercel.

## Local development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then edit for your local Postgres/Redis
uvicorn app.main:app --reload
```

Swagger docs: `http://localhost:8000/api/docs`.

## Deployment (Vercel)

Project `ecoguard-api` → FastAPI entrypoint `app/main.py` is auto-detected.
Required environment variables (Vercel project):

- `DATABASE_URL` — Neon Postgres (`postgresql+psycopg://...` with Neon's pooler host, sslmode=require)
- `REDIS_URL` — Upstash Redis (`rediss://...`), used for cache + realtime event log
- `ALLOWED_ORIGINS` — frontend origin e.g. `https://ecoguard.vercel.app`
- `COOKIE_SECURE` = `true`, `SESSION_SAMESITE` = `none` (cross-site session cookie)
- `APP_ENV` = `development`, `AUTO_CREATE_TABLES` = `true` (idempotent create_all on cold start)
- `JOBS_MODE` = `inline` (no Celery worker on serverless)
- `CSRF_REALM` note: keep `COOKIE_SECURE=true` so `SameSite=None` cookies require HTTPS.

Evidence images use the `<your-project>-private` local storage on Vercel and are
ephemeral per function instance; connect S3 credentials
(`STORAGE_BACKEND=s3` + `S3_BUCKET`/`S3_ACCESS_KEY`/`S3_SECRET_KEY`) to persist
uploads.

## Operational notes

- `/health` returns `{"status":"ok"}`.
- `AUTO_CREATE_TABLES` + `APP_ENV=development` run idempotent `create_all` at
  startup; production migration flow is via Alembic if you switch `APP_ENV=production`.