"""Read-only API for the webui. The UI never talks to the scraper or redis directly."""
import json
from datetime import datetime, timedelta, timezone

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select

from . import events
from .config import settings  # events = shared redis helper (same stream the scraper writes)
from .db import SessionLocal, init_db
from .models import Alert, Event

@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Tiketwar Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # Tailscale-only deployment; tighten if ever exposed
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/healthz")
def healthz():
    """Answerable by a machine. This is what the external watchdog polls."""
    now = datetime.now(timezone.utc)
    hb_raw, hb_age, scraper_alive = None, None, False
    try:
        r = events.connect()
        hb_raw = r.get(settings.heartbeat_key)
        if hb_raw:
            hb_ts = json.loads(hb_raw)["ts"]
            hb_age = round(now.timestamp() - hb_ts, 1)
            scraper_alive = hb_age < settings.dead_after_seconds
    except Exception as exc:                          # noqa: BLE001
        hb_raw = f"redis error: {exc}"

    db_ok = True
    try:
        with SessionLocal() as s:
            s.execute(select(func.count()).select_from(Event))
    except Exception:                                 # noqa: BLE001
        db_ok = False

    healthy = db_ok and scraper_alive
    return {
        "status": "ok" if healthy else "degraded",
        "db_ok": db_ok,
        "scraper_alive": scraper_alive,
        "heartbeat_age_seconds": hb_age,
        "heartbeat_raw": hb_raw,
        "timezone": settings.timezone,
    }

@app.get("/api/status")
def status():
    with SessionLocal() as s:
        last = s.execute(select(Event).order_by(Event.id.desc()).limit(1)).scalars().first()
        counts = dict(
            s.execute(
                select(Event.type, func.count()).group_by(Event.type)
            ).all()
        )
    return {
        "last_event": None if not last else {
            "type": last.type, "ts": last.ts, "seats": last.seats,
            "blocked": last.blocked, "detail": last.detail,
        },
        "counts_by_type": counts,
    }

@app.get("/api/events")
def list_events(limit: int = Query(100, le=1000), since_minutes: int | None = None):
    with SessionLocal() as s:
        q = select(Event).order_by(Event.id.desc()).limit(limit)
        if since_minutes:
            q = q.where(Event.ts >= datetime.now(timezone.utc) - timedelta(minutes=since_minutes))
        rows = s.execute(q).scalars().all()
    return {"events": [
        {"id": e.id, "type": e.type, "ts": e.ts, "seats": e.seats,
         "available": e.available, "blocked": e.blocked, "ok": e.ok, "detail": e.detail}
        for e in rows
    ]}

@app.get("/api/summary")
def summary(days: int = Query(7, le=90)):
    """The numbers the daily report reads. Yield decay shows up here, not in a log."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with SessionLocal() as s:
        total = s.execute(select(func.count()).select_from(Event).where(Event.ts >= since)).scalar_one()
        checks = s.execute(select(func.count()).select_from(Event)
                           .where(Event.ts >= since, Event.type == "check")).scalar_one()
        errors = s.execute(select(func.count()).select_from(Event)
                           .where(Event.ts >= since, Event.type == "poll_error")).scalar_one()
        blocked = s.execute(select(func.count()).select_from(Event)
                            .where(Event.ts >= since, Event.blocked.is_(True))).scalar_one()
        grabbed = s.execute(select(func.count()).select_from(Event)
                            .where(Event.ts >= since, Event.type == "add_to_cart",
                                   Event.ok.is_(True))).scalar_one()
        # "saw stock at least once" - the yield proxy
        saw_stock = s.execute(select(func.count()).select_from(Event)
                              .where(Event.ts >= since, Event.type == "check",
                                     Event.available.is_(True))).scalar_one()
        unknown = s.execute(select(func.count()).select_from(Event)
                            .where(Event.ts >= since, Event.type == "check",
                                   Event.seats.is_(None))).scalar_one()
    return {
        "window_days": days,
        "events_total": total, "checks": checks, "poll_errors": errors,
        "blocked_events": blocked, "grabbed_ok": grabbed,
        "checks_with_stock": saw_stock, "checks_with_unknown_seats": unknown,
        "yield_ratio": round(saw_stock / checks, 4) if checks else None,
        "selectors_probably_broken": bool(checks and unknown / checks > 0.5),
    }

@app.get("/api/alerts")
def list_alerts(limit: int = 50):
    with SessionLocal() as s:
        rows = s.execute(select(Alert).order_by(Alert.id.desc()).limit(limit)).scalars().all()
    return {"alerts": [
        {"id": a.id, "kind": a.kind, "fingerprint": a.fingerprint, "created_at": a.created_at}
        for a in rows
    ]}
