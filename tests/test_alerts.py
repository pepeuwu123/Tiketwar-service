"""Alert tests: the point of these is that we alert ONCE and stay quiet otherwise."""
import fakeredis
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import alerts
from app.db import Base
from app.models import Event

@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    S = sessionmaker(bind=engine, expire_on_commit=False)
    with S() as s:
        yield s

@pytest.fixture
def sent(monkeypatch):
    calls = []
    monkeypatch.setattr(alerts.notify, "send", lambda t, b: calls.append((t, b)) or True)
    return calls

def ev(**kw):
    return Event(stream_id=kw.pop("stream_id", "1-0"), type=kw.pop("type", "check"), **kw)

def test_poll_error_alerts_once(db, sent):
    alerts.on_event(db, ev(type="poll_error", detail="TimeoutError"))
    alerts.on_event(db, ev(type="poll_error", detail="TimeoutError", stream_id="2-0"))
    assert len(sent) == 1 and "failing" in sent[0][0]

def test_blocked_alerts_and_says_do_not_just_restart(db, sent):
    alerts.on_event(db, ev(type="check", blocked=True, detail="captcha"))
    assert len(sent) == 1
    assert "human" in sent[0][0]
    assert "not retrying harder" in sent[0][1]

def test_normal_sold_out_is_silent(db, sent):
    """The most common state must never generate a message, or the channel gets muted."""
    for i in range(20):
        alerts.on_event(db, ev(type="check", seats=0, available=False, stream_id=f"{i}-0"))
    assert sent == []

def test_grabbed_alerts_and_refuses_to_automate_payment(db, sent):
    alerts.on_event(db, ev(type="add_to_cart", ok=True, detail="seat A001 held", seats=0))
    assert len(sent) == 1
    assert "payment is never automated" in sent[0][1]

def test_dead_scraper_detected_from_missing_heartbeat(db, sent, monkeypatch):
    from app import config
    monkeypatch.setattr(config.settings, "dead_after_seconds", 1)
    r = fakeredis.FakeRedis(decode_responses=True)
    alerts.check_heartbeat(db, r)          # key absent -> dead
    alerts.check_heartbeat(db, r)          # must not shout twice
    assert len(sent) == 1 and "DEAD" in sent[0][0]

def test_live_heartbeat_is_silent(db, sent):
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("tiketwar:heartbeat", '{"ts": 1}')
    alerts.check_heartbeat(db, r)
    assert sent == []

def test_long_outage_sends_dead_once_then_recovered_once(db, sent):
    """The check runs every 30s. With the old cooldown a one-hour outage sent DEAD
    24 times and never said it was over - the pattern that gets a channel muted."""
    r = fakeredis.FakeRedis(decode_responses=True)
    for _ in range(120):                      # an hour of checks with no heartbeat
        alerts.check_heartbeat(db, r)
    r.set("tiketwar:heartbeat", '{"ts": 1}')
    for _ in range(10):
        alerts.check_heartbeat(db, r)
    assert [t for t, _ in sent] == ["Tiketwar: scraper is DEAD", "Tiketwar: scraper RECOVERED"]
