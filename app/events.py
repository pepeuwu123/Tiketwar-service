"""Shared redis helpers. The service reads the same stream the scraper writes."""
import redis
from .config import settings

def connect(_settings=None) -> "redis.Redis":
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)
