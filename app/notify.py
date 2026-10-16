"""One interface, several channels. If nothing is configured, alerts go to the log only."""
import logging

import httpx

log = logging.getLogger("tiketwar.notify")

def send(title: str, body: str) -> bool:
    from .config import settings

    sent = False
    if settings.discord_webhook_url:
        try:
            httpx.post(settings.discord_webhook_url,
                       json={"content": f"**{title}**\n{body}"}, timeout=10.0)
            sent = True
        except Exception as exc:                     # noqa: BLE001
            log.error("discord webhook failed: %s", exc)
    if settings.ntfy_url:
        try:
            httpx.post(settings.ntfy_url, content=body.encode(),
                       headers={"Title": title, "Priority": "high"}, timeout=10.0)
            sent = True
        except Exception as exc:                     # noqa: BLE001
            log.error("ntfy failed: %s", exc)
    if settings.telegram_bot_token and settings.telegram_chat_id:
        try:
            httpx.post(
                f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
                json={"chat_id": settings.telegram_chat_id, "text": f"{title}\n{body}"},
                timeout=10.0,
            )
            sent = True
        except Exception as exc:                     # noqa: BLE001
            log.error("telegram failed: %s", exc)

    if not sent:
        log.warning("ALERT (no channel configured) %s | %s", title, body)
    return sent
