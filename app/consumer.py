"""Redis Streams -> Postgres -> alert rules. Runs as its own process beside the API."""
import json
import logging
import time
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import redis as redis_lib

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

def reclaim_stale(r, min_idle_ms: int = 60000) -> int:
    """Take back messages that a dead consumer was holding.

    Redis keeps a Pending Entries List: a message delivered to consumer A stays
    pending until A acks it. If A dies first (crash, redeploy, OOM), that message is
    stuck forever - readable in the stream, never written to Postgres, and invisible
    on the dashboard. Observed for real: 10 events stranded by a crash-looping
    consumer. XAUTOCLAIM also increments the delivery count, so a message that keeps
    killing consumers is visible rather than silently retried forever.
    """
    claimed = 0
    start = "0-0"
    while True:
        try:
            resp = r.xautoclaim(settings.stream_key, settings.consumer_group,
                                settings.consumer_name, min_idle_time=min_idle_ms,
                                start_id=start, count=100)
        except Exception as exc:                      # noqa: BLE001 - redis < 6.2
            log.warning("XAUTOCLAIM unavailable (%s); stranded messages stay pending", exc)
            return claimed
        start, messages = resp[0], resp[1]
        if not messages:
            break
        claimed += len(messages)
        with db.SessionLocal() as session:
            for stream_id, fields in messages:
                try:
                    handle(session, r, stream_id, json.loads(fields["payload"]))
                    r.xack(settings.stream_key, settings.consumer_group, stream_id)
                except Exception:                     # noqa: BLE001
                    log.exception("could not reclaim %s", stream_id)
                    session.rollback()
        if start in ("0-0", "0-0-0", None):
            break
    if claimed:
        log.info("reclaimed %d stranded message(s)", claimed)
    return claimed

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

_last_hb_check = 0.0

def consume_once(r, session_factory=None) -> int:
    """One blocking read and dispatch. Returns how many messages were handled.

    A blocking XREADGROUP raising TimeoutError is NORMAL: it means the block elapsed
    with no message. Letting that kill the process made the consumer crash-loop
    (21 restarts in one session) - and a consumer that dies whenever the queue is
    quiet is worse than no consumer, because events pile up while it restarts.
    """
    global _last_hb_check
    session_factory = session_factory or db.SessionLocal

    try:
        resp = r.xreadgroup(settings.consumer_group, settings.consumer_name,
                            {settings.stream_key: ">"}, count=50, block=5000)
    except redis_lib.exceptions.TimeoutError:
        return 0                                   # idle, not broken

    handled = 0
    with session_factory() as session:
        for _stream, messages in resp or []:
            for stream_id, fields in messages:
                try:
                    payload = json.loads(fields["payload"])
                    handle(session, r, stream_id, payload)
                    r.xack(settings.stream_key, settings.consumer_group, stream_id)
                    handled += 1
                except Exception:                  # noqa: BLE001
                    # One malformed message must not take down the consumer.
                    log.exception("failed to handle %s; leaving it unacked", stream_id)
                    session.rollback()

        now = time.monotonic()
        if now - _last_hb_check > 30:
            try:
                alerts.check_heartbeat(session, r)
            except Exception:                      # noqa: BLE001
                log.exception("heartbeat check failed")
            _last_hb_check = now
    return handled

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    db.init_db()
    r = events.connect(settings)
    ensure_group(r)
    reclaim_stale(r)
    log.info("consumer %s up on %s", settings.consumer_name, settings.stream_key)
    last_reclaim = time.monotonic()
    while True:
        try:
            consume_once(r)
            # Heal again periodically: a consumer can die between startups too.
            if time.monotonic() - last_reclaim > 300:
                reclaim_stale(r)
                last_reclaim = time.monotonic()
        except Exception as exc:                   # noqa: BLE001
            log.warning("consumer loop error (%s); retrying in 2s", exc)
            time.sleep(2)

if __name__ == "__main__":
    main()
