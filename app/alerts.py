"""Alert rules. Deliberately few, and each one is deduplicated.

A monitor that cries wolf gets muted, and a muted monitor is worse than none.
So: only alert on things that are actually actionable, and only once per occurrence.
"""
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from . import notify
from .models import Alert

log = logging.getLogger("tiketwar.alerts")

def _already_sent(db, kind: str, fingerprint: str, cooldown_seconds: int = 3600) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=cooldown_seconds)
    row = db.execute(
        select(Alert).where(Alert.kind == kind, Alert.fingerprint == fingerprint)
        .where(Alert.created_at >= cutoff).order_by(Alert.id.desc())
    ).scalars().first()
    if row:
        return True
    db.add(Alert(kind=kind, fingerprint=fingerprint))
    db.commit()
    return False

def on_event(db, event) -> None:
    """Rules driven by incoming scraper events."""
    if event.type == "poll_error":
        if not _already_sent(db, "poll_error", event.detail or "unknown", 1800):
            notify.send("Tiketwar: scrape failing",
                        f"The scraper errored: {event.detail}\n"
                        "Likely a rotted selector or the site changed. Nothing was grabbed.")

    elif event.type == "check" and event.blocked:
        if not _already_sent(db, "blocked", event.detail or "blocked", 3600):
            notify.send("Tiketwar: BLOCKED, needs a human",
                        f"Reason: {event.detail}\n"
                        "The scraper stopped and is not retrying harder, on purpose. "
                        "Decide what happens next - do not just restart it.")

    elif event.type == "add_to_cart" and event.ok:
        if not _already_sent(db, "grabbed", str(event.seats), 300):
            notify.send("Tiketwar: seats added to cart",
                        f"{event.detail}\nGo finish the checkout yourself - "
                        "payment is never automated.")

    elif event.type == "check" and not event.blocked and event.seats == 0:
        # Informational only, and rate-limited hard: this is the normal "sold out" state.
        pass

# Set while the scraper is known dead. Lives in redis next to the heartbeat so a
# consumer restart mid-outage neither re-sends DEAD nor forgets to send RECOVERED.
DEAD_FLAG_KEY = "tiketwar:alert:scraper_dead"

def check_heartbeat(db, redis_client) -> None:
    """The failure a plain ping test can't see: box up, scraper silent.

    Fires on transitions only, like the watchdog: alive -> dead sends DEAD once,
    dead -> alive sends RECOVERED once. It used to be a cooldown equal to
    dead_after_seconds, which re-sent DEAD every ~2.5 minutes for as long as the
    outage lasted (24 messages in an hour) and never said when it was over.
    """
    from .config import settings

    alive = bool(redis_client.exists(settings.heartbeat_key))
    flagged = bool(redis_client.exists(DEAD_FLAG_KEY))
    if not alive and not flagged:
        redis_client.set(DEAD_FLAG_KEY, "1")
        db.add(Alert(kind="dead_scraper", fingerprint="heartbeat_missing"))
        db.commit()
        notify.send("Tiketwar: scraper is DEAD",
                    f"No heartbeat for {settings.dead_after_seconds}s. "
                    "The container is probably up but the loop is not running. "
                    "This is the silent-failure mode - check logs.")
    elif alive and flagged:
        redis_client.delete(DEAD_FLAG_KEY)
        db.add(Alert(kind="scraper_recovered", fingerprint="heartbeat_back"))
        db.commit()
        notify.send("Tiketwar: scraper RECOVERED",
                    "The heartbeat is back. Check the dashboard for anything missed while it was down.")
