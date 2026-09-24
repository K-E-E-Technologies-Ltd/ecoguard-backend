"""Redis-backed cache and realtime update channel.

The app falls back to a process-local cache when no Redis is configured so
local development, tests and minimal deployments keep working. In production
the Upstash/Redis connection (REDIS_URL, rediss:// style) is used for both
short-lived API caching and a rolling list of recent realtime events.
"""
import asyncio
import json
import threading
import time
from typing import Any, Callable, Optional

from .config import get_settings


def _redis_client():
    cfg = get_settings()
    url = (cfg.redis_url or '').strip()
    if url.startswith('redis'):
        import redis
        return redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2,
                                    decode_responses=True)
    return None


class MemoryCache:
    def __init__(self):
        self._data: dict = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
        if not item:
            return None
        if item[1] < time.time():
            with self._lock:
                self._data.pop(key, None)
            return None
        return item[0]

    def set(self, key, value, ttl):
        with self._lock:
            self._data[key] = (value, time.time() + ttl)

    def delete(self, prefix: Optional[str]):
        with self._lock:
            if prefix is None:
                self._data.clear()
                return
            for key in list(self._data):
                if key.startswith(prefix):
                    self._data.pop(key, None)


class Cache:
    def __init__(self):
        self._client = None
        self._mem = MemoryCache()

    @property
    def redis(self):
        if self._client is None:
            try:
                self._client = _redis_client()
            except Exception:
                self._client = False
        return self._client if self._client is not None else None

    def get(self, key):
        client = self.redis
        if client is not None:
            try:
                raw = client.get(key)
                if raw is None:
                    return None
                return json.loads(raw)
            except Exception:
                return None
        return self._mem.get(key)

    def set(self, key, value, ttl):
        client = self.redis
        if client is not None:
            try:
                client.set(key, json.dumps(value), ex=ttl)
            except Exception:
                pass
        else:
            self._mem.set(key, value, ttl)

    def delete(self, prefix: Optional[str]):
        client = self.redis
        if client is not None:
            try:
                keys = list(client.scan_iter(match=(prefix or '') + '*'))
                if keys:
                    client.delete(*keys)
            except Exception:
                pass
        self._mem.delete(prefix)

    def cached(self, key, ttl, loader: Callable[[], Any]):
        value = self.get(key)
        if value is not None:
            return value
        value = loader()
        self.set(key, value, ttl)
        return value


cache = Cache()


class Hub:
    """Fan-out of realtime events to in-flight SSE connections."""

    def __init__(self):
        self._subs = []
        self._lock = threading.Lock()

    def publish(self, event: dict):
        data = dict(event or {})
        client = cache.redis
        if client is not None:
            try:
                seq = client.incr('ecoguard:events:seq')
                data['id'] = int(seq)
                client.lpush('ecoguard:events', json.dumps(data, separators=(',', ':')))
                client.ltrim('ecoguard:events', 0, 199)
            except Exception:
                pass
        data.setdefault('id', 'local-%.3f' % time.time())
        with self._lock:
            subscribers = list(self._subs)
        for queue in subscribers:
            try:
                queue.put_nowait(data)
            except Exception:
                pass

    def subscribe(self):
        queue = asyncio.Queue()
        with self._lock:
            self._subs.append(queue)

        async def stream():
            try:
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=15)
                        yield 'data: ' + json.dumps(event, separators=(',', ':')) + '\n\n'
                    except asyncio.TimeoutError:
                        yield ': ping\n\n'
            finally:
                with self._lock:
                    if queue in self._subs:
                        self._subs.remove(queue)

        return stream()


hub = Hub()


def publish(event: dict):
    hub.publish(event)


def invalidate(*prefixes):
    for prefix in prefixes:
        cache.delete(prefix)


def touch(kind: str, object_id: str = ''):
    """Invalidate shared caches and broadcast a change in one call."""
    invalidate('advisories:', 'map:', 'dashboard:', 'config:', 'areas:')
    publish({'type': kind, 'object_id': object_id,
             'at': time.time(), 'event': kind})