# Tiketwar-service

Turns scraper events into history, state, and **one alert per real problem**.

- `app/consumer.py` reads the redis stream the scraper writes (`tiketwar:events`)
  and appends every event to Postgres. Append-only = the audit trail.
- `app/alerts.py` holds the alert rules. They are few and deduplicated on purpose:
  **a monitor that cries wolf gets muted, and a muted monitor is worse than none.**
- `app/api.py` is the only thing the webui talks to, and it is read-only.
- `GET /healthz` is what the external watchdog polls. It reports `scraper_alive`
  from the scraper's heartbeat key, so "box up, scraper silent" is detectable, and
  `consumer_alive` from the consumer's own heartbeat (`tiketwar:consumer_heartbeat`,
  written on every loop, idle reads included). `status` is `degraded` unless the
  database, the scraper and the consumer are all alive.
- The consumer deletes events older than `RETENTION_DAYS` (default 90, the longest
  `/api/summary` window), at startup and then hourly, in batches.

## The failure modes it distinguishes
| Symptom | Meaning |
|---|---|
| `poll_error` | site changed / selectors rotted |
| `blocked` | HTTP 403/429 or a captcha marker — **needs a human**; the scraper backs off instead of retrying harder |
| heartbeat missing | container up but the loop is not running (the silent one) |
| `consumer_alive: false` | events are piling up in redis and not reaching Postgres; the dashboard is stale |

Scraper heartbeat alerts fire on transitions only: one DEAD when it goes missing,
one RECOVERED when it comes back. A dead consumer has no alert of its own here:
`/healthz` turns `degraded`, and the external watchdog sends DOWN and RECOVERED.

## Run it
```sh
pip install -e '.[dev]'
pytest
uvicorn app.api:app --reload --port 8080   # API
python -m app.consumer                     # consumer
```
Settings come from environment variables only (see `.env.example`). The defaults
point at the compose hostnames `postgres` and `redis`, so outside the
`Tiketwar-infra` stack set `DATABASE_URL` and `REDIS_URL` first.
