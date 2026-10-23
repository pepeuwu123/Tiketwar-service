"""One interface, several channels. If nothing is configured, alerts go to the log only."""
import logging

import httpx

log = logging.getLogger("tiketwar.notify")

def _post(name: str, url: str, **kw) -> bool:
    """True only if the channel accepted the message.

    A 4xx/5xx used to count as delivered - a typo'd webhook or a revoked token meant
    every alert vanished while the code believed it had sent them. Errors are logged
    by type and status only: the Telegram URL contains the bot token.
    """
    try:
        httpx.post(url, timeout=10.0, **kw).raise_for_status()
        return True
    except httpx.HTTPStatusError as exc:
        log.error("%s rejected the alert: HTTP %s", name, exc.response.status_code)
    except Exception as exc:                         # noqa: BLE001
        log.error("%s failed: %s", name, type(exc).__name__)
    return False

def send(title: str, body: str) -> bool:
    from .config import settings

    sent = False
    if settings.discord_webhook_url:
        sent |= _post("discord webhook", settings.discord_webhook_url,
                      json={"content": f"**{title}**\n{body}"})
    if settings.ntfy_url:
        sent |= _post("ntfy", settings.ntfy_url, content=body.encode(),
                      headers={"Title": title, "Priority": "high"})
    if settings.telegram_bot_token and settings.telegram_chat_id:
        sent |= _post("telegram",
                      f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
                      json={"chat_id": settings.telegram_chat_id, "text": f"{title}\n{body}"})

    if not sent:
        # Also reached when every configured channel failed, so the alert is never lost.
        log.warning("ALERT (no channel delivered) %s | %s", title, body)
    return sent
