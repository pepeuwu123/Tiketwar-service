# Tiketwar-service

Turns scraper events into history, state, and **one alert per real problem**.

- `app/consumer.py` reads the redis stream the scraper writes (`tiketwar:events`)
  and appends every event to Postgres. Append-only = the audit trail.
- `app/alerts.py` holds the alert rules. They are few and deduplicated on purpose:
  **a monitor that cries wolf gets muted, and a muted monitor is worse than none.**
- `app/api.py` is the only thing the webui talks to, and it is read-only.
- `GET /healthz` is what the external watchdog polls. It reports `scraper_alive`
  from the heartbeat key, so "box up, scraper silent" is detectable.

## The three failure modes it distinguishes
| Symptom | Meaning |
|---|---|
| `poll_error` | site changed / selectors rotted |
| `blocked` | captcha / login wall / 429 — **needs a human, it will not retry harder** |
| heartbeat missing | container up but the loop is not running (the silent one) |

## Run it
```sh
pip install -e '.[dev]'
uvicorn app.api:app --reload --port 8080   # API
python -m app.consumer                     # consumer
```
