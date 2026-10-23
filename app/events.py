"""Shared redis helpers. The service reads the same stream the scraper writes."""
import redis
from .config import settings

_client: "redis.Redis | None" = None

def connect(_settings=None) -> "redis.Redis":
    """One client per process. /healthz used to build a new client and connection
    pool on every request - every 10s from the dashboard, plus every healthcheck."""
    global _client
    if _client is None:
        _client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    return _client
