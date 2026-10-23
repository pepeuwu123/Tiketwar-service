"""End-to-end-ish: a scraper payload must land in Postgres exactly once and be readable via the API."""
import json
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import consumer, db as dbmod
from app.db import Base
from app.models import Event

@pytest.fixture
def session(monkeypatch):
    # StaticPool keeps ONE connection, so the TestClient thread and this session see
    # the same in-memory database. Without it, :memory: is per-connection and the API
    # reads an empty schema while the test writes into a different one.
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    S = sessionmaker(bind=engine, expire_on_commit=False)
    # db.py is lazy on purpose: no psycopg needed for a SQLite test.
    monkeypatch.setattr(dbmod, "get_engine", lambda: engine)
    monkeypatch.setattr(dbmod, "_SessionLocal", S)
    with S() as s:
        yield s

def payload(**kw):
    return {"type": "check", "ts": 1700000000.0, "state": {"seats": 5, "available": True, **kw}}

def test_event_is_persisted(session, monkeypatch):
    monkeypatch.setattr(consumer.alerts, "on_event", lambda db, ev: None)
    consumer.handle(session, None, "1-0", payload())
    row = session.execute(select(Event)).scalars().one()
    assert row.seats == 5 and row.available is True and row.type == "check"

def test_replayed_stream_id_is_not_duplicated(session, monkeypatch):
    """Streams redeliver. A duplicate row would double-count yield in the daily report."""
    monkeypatch.setattr(consumer.alerts, "on_event", lambda db, ev: None)
    consumer.handle(session, None, "1-0", payload())
    consumer.handle(session, None, "1-0", payload(seats=9))
    rows = session.execute(select(Event)).scalars().all()
    assert len(rows) == 1 and rows[0].seats == 5

def test_unknown_seats_survive_as_null_not_zero(session, monkeypatch):
    """If unknown collapsed to 0 it would be indistinguishable from sold out."""
    monkeypatch.setattr(consumer.alerts, "on_event", lambda db, ev: None)
    consumer.handle(session, None, "2-0", {"type": "check", "ts": 1.0, "state": {"seats": None}})
    row = session.execute(select(Event)).scalars().one()
    assert row.seats is None

def test_healthz_reports_dead_scraper_when_heartbeat_missing(session):
    from app import api
    client = TestClient(api.app)
    body = client.get("/healthz").json()
    assert body["status"] == "degraded"
    assert body["scraper_alive"] is False

def test_summary_flags_broken_selectors(session):
    """Unknown seat counts are the fingerprint of rotted selectors, and must be visible in the summary.

    Timestamps are relative to now on purpose: a hardcoded epoch silently falls
    outside the summary window and the test passes for the wrong reason.
    """
    from app import api
    client = TestClient(api.app)
    now = time.time()
    for i in range(4):
        consumer.handle(session, None, f"{i}-0", {"type": "check", "ts": now,
                                                  "state": {"seats": None, "available": False}})
    body = client.get("/api/summary?days=7").json()
    assert body["checks_with_unknown_seats"] == 4
    assert body["selectors_probably_broken"] is True

def test_summary_does_not_flag_selectors_when_seats_are_readable(session):
    from app import api
    client = TestClient(api.app)
    now = time.time()
    for i in range(4):
        consumer.handle(session, None, f"ok-{i}-0", {"type": "check", "ts": now,
                                                     "state": {"seats": 3, "available": True}})
    body = client.get("/api/summary?days=7").json()
    assert body["checks_with_unknown_seats"] == 0
    assert body["selectors_probably_broken"] is False
    assert body["yield_ratio"] == 1.0

def test_summary_rejects_absurd_windows(session):
    """Guards the daily report against asking for a nonsense range."""
    from app import api
    client = TestClient(api.app)
    assert client.get("/api/summary?days=36500").status_code == 422


def test_healthz_distinguishes_alive_but_failing_from_dead(session, monkeypatch):
    """A rotted selector must NOT look like a dead scraper.

    Both used to be reported as DEAD, which sends you to debug the wrong
    component. Now: heartbeat present = loop alive; last_poll_ok = site healthy.
    """
    import fakeredis
    from app import api, events

    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("tiketwar:heartbeat", json.dumps({"ts": time.time(), "ok": False, "state": None}))
    monkeypatch.setattr(events, "connect", lambda _s=None: r)

    body = TestClient(api.app).get("/healthz").json()
    assert body["scraper_alive"] is True, "the loop is running"
    assert body["last_poll_ok"] is False, "but the last poll failed - different problem"

def test_healthz_reports_healthy_poll_separately(session, monkeypatch):
    import fakeredis
    from app import api, events

    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("tiketwar:heartbeat", json.dumps({"ts": time.time(), "ok": True, "state": {"seats": 1}}))
    monkeypatch.setattr(events, "connect", lambda _s=None: r)

    body = TestClient(api.app).get("/healthz").json()
    assert body["scraper_alive"] is True and body["last_poll_ok"] is True
    assert body["status"] == "ok"

def test_consume_once_survives_an_idle_timeout(session, monkeypatch):
    """A blocking XREADGROUP with no messages raises TimeoutError. That is idle, not broken.

    Letting it escape made the consumer crash-loop: 21 restarts observed in one run,
    and every restart window means events are not being consumed.
    """
    import fakeredis
    from app import consumer

    r = fakeredis.FakeRedis(decode_responses=True)
    import redis as redis_lib
    def raise_timeout(*a, **kw):
        raise redis_lib.exceptions.TimeoutError("timed out")
    monkeypatch.setattr(r, "xreadgroup", raise_timeout)
    assert consumer.consume_once(r, session_factory=lambda: session.__class__(bind=session.get_bind())) == 0

def test_consume_once_dispatches_messages(session, monkeypatch):
    import fakeredis
    from app import consumer

    r = fakeredis.FakeRedis(decode_responses=True)
    payload = {"type": "check", "ts": time.time(), "state": {"seats": 2, "available": True}}
    monkeypatch.setattr(r, "xreadgroup",
                        lambda *a, **kw: [("tiketwar:events", [("9-0", {"payload": json.dumps(payload)})])])
    monkeypatch.setattr(r, "xack", lambda *a, **kw: 1)
    monkeypatch.setattr(consumer.alerts, "on_event", lambda db, ev: None)
    n = consumer.consume_once(r, session_factory=lambda: session.__class__(bind=session.get_bind()))
    assert n == 1

def test_consume_once_survives_a_malformed_message(session, monkeypatch):
    """One bad payload must not kill the loop and strand every later message."""
    import fakeredis
    from app import consumer

    r = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(r, "xreadgroup",
                        lambda *a, **kw: [("tiketwar:events", [("9-0", {"payload": "not json"})])])
    monkeypatch.setattr(r, "xack", lambda *a, **kw: 1)
    n = consumer.consume_once(r, session_factory=lambda: session.__class__(bind=session.get_bind()))
    assert n == 0

def test_idle_stream_still_checks_the_heartbeat(session, monkeypatch):
    """Found on the live stack: with the scraper stopped, the consumer never said DEAD.

    A dead scraper means an idle stream, an idle read raises TimeoutError, and
    consume_once returned before reaching the heartbeat check - so the alert was
    skipped exactly when it was needed.
    """
    import fakeredis
    import redis as redis_lib
    from app import consumer

    r = fakeredis.FakeRedis(decode_responses=True)          # no heartbeat key: scraper dead
    def raise_timeout(*a, **kw):
        raise redis_lib.exceptions.TimeoutError("timed out")
    monkeypatch.setattr(r, "xreadgroup", raise_timeout)
    monkeypatch.setattr(consumer, "_last_hb_check", 0.0)
    sent = []
    monkeypatch.setattr(consumer.alerts.notify, "send", lambda t, b: sent.append(t) or True)
    consumer.consume_once(r, session_factory=lambda: session.__class__(bind=session.get_bind()))
    assert sent == ["Tiketwar: scraper is DEAD"]

def test_malformed_message_is_acked_not_retried_forever(session):
    """Retrying cannot fix bad JSON. Unacked, it was re-claimed and re-logged every
    reclaim pass for the life of the stream."""
    import fakeredis
    from app import consumer
    from app.config import settings

    r = fakeredis.FakeRedis(decode_responses=True)
    consumer.ensure_group(r)
    r.xadd(settings.stream_key, {"payload": "{not json"})
    r.xadd(settings.stream_key, {"payload": "[1, 2]"})
    consumer.consume_once(r, session_factory=lambda: session.__class__(bind=session.get_bind()))
    assert r.xpending(settings.stream_key, settings.consumer_group)["pending"] == 0

def test_stranded_messages_are_reclaimed(session, monkeypatch):
    """A message held by a consumer that died must not be lost forever.

    Redis keeps delivered-but-unacked messages pending. If the consumer dies first
    they stay pending indefinitely - in the stream, invisible in Postgres. Observed
    for real: 10 events stranded by a crash-looping consumer (dead consumer id in
    XPENDING), a permanent gap between XLEN and the row count.
    """
    from app import consumer

    payload = {"type": "check", "ts": time.time(), "state": {"seats": 4, "available": True}}
    fake = type("R", (), {})()
    fake.xautoclaim = lambda *a, **kw: ["0-0", [("7-0", {"payload": json.dumps(payload)})]]
    acked = []
    fake.xack = lambda *a, **kw: acked.append(a[2])
    monkeypatch.setattr(consumer.alerts, "on_event", lambda db, ev: None)

    n = consumer.reclaim_stale(fake, min_idle_ms=0)
    assert n == 1
    assert acked == ["7-0"], "a reclaimed message must be acked or it stays pending forever"

def test_reclaim_survives_missing_xautoclaim(session, monkeypatch, caplog):
    """Redis older than 6.2 has no XAUTOCLAIM; the consumer must still start."""
    from app import consumer
    fake = type("R", (), {})()
    def boom(*a, **kw):
        raise AttributeError("no such method")
    fake.xautoclaim = boom
    assert consumer.reclaim_stale(fake, min_idle_ms=0) == 0
