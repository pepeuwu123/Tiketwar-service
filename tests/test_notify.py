"""A channel that rejects the alert must not count as delivered."""
import httpx

from app import notify
from app.config import settings

def _reply(status):
    return lambda url, **kw: httpx.Response(status, request=httpx.Request("POST", url))

def test_rejected_webhook_is_not_delivered(monkeypatch, caplog):
    """A typo'd webhook or a revoked token used to count as sent, so every alert vanished."""
    monkeypatch.setattr(settings, "discord_webhook_url", "https://discord.example/hook")
    monkeypatch.setattr(httpx, "post", _reply(404))
    assert notify.send("t", "b") is False
    assert "HTTP 404" in caplog.text
    assert "ALERT (no channel delivered) t | b" in caplog.text, "the alert must still reach the log"

def test_accepted_webhook_is_delivered(monkeypatch):
    monkeypatch.setattr(settings, "discord_webhook_url", "https://discord.example/hook")
    monkeypatch.setattr(httpx, "post", _reply(204))
    assert notify.send("t", "b") is True

def test_telegram_token_never_reaches_the_log(monkeypatch, caplog):
    monkeypatch.setattr(settings, "telegram_bot_token", "123:SECRET")
    monkeypatch.setattr(settings, "telegram_chat_id", "42")
    monkeypatch.setattr(httpx, "post", _reply(401))
    notify.send("t", "b")
    assert "SECRET" not in caplog.text
