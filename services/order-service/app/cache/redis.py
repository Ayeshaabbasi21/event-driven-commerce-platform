import json
import logging
from typing import Any

import redis
from redis.exceptions import RedisError

from app.core.config import settings

logger = logging.getLogger(__name__)

redis_client = redis.Redis.from_url(
    settings.redis_url,
    decode_responses=True,
)


def get_cached_order(order_id: int) -> dict[str, Any] | None:
    """Return a cached order or None when the cache is unavailable/missing."""
    key = f"order:{order_id}"

    try:
        cached_order = redis_client.get(key)
    except RedisError:
        logger.warning(
            "Redis unavailable while reading cache for order_id=%s",
            order_id,
            exc_info=True,
        )
        return None

    if cached_order is None:
        return None

    return json.loads(cached_order)


def cache_order(
    order_id: int,
    order_data: dict[str, Any],
    ttl_seconds: int = 300,
) -> None:
    """Store an order in Redis with a time-to-live."""
    key = f"order:{order_id}"

    try:
        redis_client.setex(
            key,
            ttl_seconds,
            json.dumps(order_data),
        )
    except RedisError:
        logger.warning(
            "Redis unavailable while caching order_id=%s",
            order_id,
            exc_info=True,
        )


def delete_cached_order(order_id: int) -> None:
    """Remove an order from Redis after a state-changing operation."""
    key = f"order:{order_id}"

    try:
        redis_client.delete(key)
    except RedisError:
        logger.warning(
            "Redis unavailable while deleting cache for order_id=%s",
            order_id,
            exc_info=True,
        )