import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://tiketwar:tiketwar@postgres:5432/tiketwar"
    redis_url: str = "redis://redis:6379/0"
    stream_key: str = "tiketwar:events"
    heartbeat_key: str = "tiketwar:heartbeat"
    consumer_group: str = "tiketwar-service"
    consumer_name: str = os.environ.get("HOSTNAME", "service-1")
    # Alerting: set one. Silent if none is set (by design - no fake confidence).
    discord_webhook_url: str | None = None
    ntfy_url: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    dead_after_seconds: int = 120
    timezone: str = "Asia/Jakarta"

settings = Settings()
