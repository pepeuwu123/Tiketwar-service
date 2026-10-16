"""Redis Streams -> Postgres -> alert rules. Runs as its own process beside the API."""
import json
import logging
import socket
import time
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from . import alerts, db, events
from .config import settings
from .models import Event

log = logging.getLogger("tiketwar.consumer")

def ensure_group(r) -> None:
    try:
        r.xgroup_create(settings.stream_key, settings.consumer_group, id="0", mkstream=True)
    except Exception as exc:                          # noqa: BLE001 - BUSYGROUP means it already exists
        if "BUSYGROUP" not in str(exc):
            raise

def handle(db_session, redis_client, stream_id: str, payload: dict) -> None:
    state = payload.get("state") or {}
    ev = Event(
        stream_id=stream_id,
        type=payload.get("type", "unknown"),
        ts=datetime.fromtimestamp(payload.get("ts", time.time()), tz=timezone.utc),
        seats=state.get("seats", payload.get("seats")),
        available=state.get("available"),
        blocked=bool(state.get("blocked", False)),
        ok=payload.get("ok"),
        detail=state.get("block_reason") or payload.get("detail") or payload.get("error"),
        raw=json.dumps(payload)[:4000],
    )
    try:
        db_session.add(ev)
        db_session.commit()
    except IntegrityError:
        db_session.rollback()
        return                                        # already handled - streams redeliver
    alerts.on_event(db_session, ev)

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    db.init_db()
    r = events.connect(settings)
    ensure_group(r)
    log.info("consumer %s up on %s", settings.consumer_name, settings.stream_key)
    last_hb_check = 0.0
    while True:
        resp = r.xreadgroup(settings.consumer_group, settings.consumer_name,
                            {settings.stream_key: ">"}, count=50, block=5000)
        with db.SessionLocal() as session:
            for _stream, messages in resp or []:
                for stream_id, fields in messages:
                    payload = json.loads(fields["payload"])
                    handle(session, r, stream_id, payload)
                    r.xack(settings.stream_key, settings.consumer_group, stream_id)

            now = time.monotonic()
            if now - last_hb_check > 30:
                alerts.check_heartbeat(session, r)
                last_hb_check = now

if __name__ == "__main__":
    main()
