"""
Local server for the dashboard.

One background task polls the feed and caches it, so ten open tabs still only
cost the feed one request per cycle.

Run with:
    python -m uvicorn server:app --reload
then open http://127.0.0.1:8000/

(Use `python -m uvicorn` - a bare `uvicorn` often isn't on PATH.)
"""

import asyncio
import re
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from main import DEBUG_URL, fetch_debug_text, parse_vehicles

BASE_DIR = Path(__file__).parent

# The feed only republishes every ~15s, so polling faster is wasted bandwidth.
POLL_INTERVAL_SECONDS = 15

# Trains only for now - add "tram" and "bus" here to go network-wide.
LIVE_TYPES = ("train",)

# Tells us how old the data is, not just when we last asked for it.
HEADER_TIMESTAMP_RE = re.compile(r"timestamp:\s*(\d+)")


def parse_feed_timestamp(text: str) -> int | None:
    # Slice off the entities first or we'd match a vehicle's own timestamp.
    header_block = text.split("entity {", 1)[0]
    match = HEADER_TIMESTAMP_RE.search(header_block)
    return int(match.group(1)) if match else None


class FeedCache:
    """Last successful poll, plus whatever went wrong since."""

    def __init__(self) -> None:
        self.vehicles: list[dict] = []
        self.feed_timestamp: int | None = None
        self.updated_at: str | None = None
        # Monotonic so the countdown doesn't jump if the system clock changes.
        self.fetched_at: float | None = None
        self.error: str | None = None

    def store(self, vehicles: list[dict], feed_timestamp: int | None) -> None:
        self.vehicles = vehicles
        self.feed_timestamp = feed_timestamp
        self.updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.fetched_at = time.monotonic()
        self.error = None

    def seconds_until_refresh(self) -> float:
        if self.fetched_at is None:
            return 0.0
        elapsed = time.monotonic() - self.fetched_at
        return max(0.0, POLL_INTERVAL_SECONDS - elapsed)

    def age_seconds(self) -> float | None:
        if self.fetched_at is None:
            return None
        return time.monotonic() - self.fetched_at


async def poll_feed(cache: FeedCache) -> None:
    """Refresh the cache forever, one fetch per POLL_INTERVAL_SECONDS."""
    while True:
        started = time.monotonic()
        try:
            # urllib blocks, so run it off the event loop or requests stall.
            text = await asyncio.to_thread(fetch_debug_text, DEBUG_URL)
            vehicles = parse_vehicles(text)
            cache.store(vehicles, parse_feed_timestamp(text))
            print(f"Polled feed: {len(vehicles)} vehicles at {cache.updated_at}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Keep the old snapshot - stale positions beat a blank map.
            cache.error = f"{type(exc).__name__}: {exc}"
            print(f"Feed poll failed: {cache.error}")

        # Subtract the fetch time so the cycle stays on a steady 15s beat.
        await asyncio.sleep(max(1.0, POLL_INTERVAL_SECONDS - (time.monotonic() - started)))


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = FeedCache()
    app.state.cache = cache
    task = asyncio.create_task(poll_feed(cache))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Adelaide Metro Dashboard", lifespan=lifespan)


@app.get("/api/vehicles")
async def api_vehicles() -> dict:
    """Live vehicles plus the timing info the client's countdown runs off."""
    cache: FeedCache = app.state.cache
    live = [v for v in cache.vehicles if v["type"] in LIVE_TYPES]
    return {
        "live_types": list(LIVE_TYPES),
        "vehicles": live,
        "count": len(live),
        "feed_timestamp": cache.feed_timestamp,
        "updated_at": cache.updated_at,
        "age_seconds": round(cache.age_seconds(), 1) if cache.age_seconds() is not None else None,
        "next_refresh_in": round(cache.seconds_until_refresh(), 1),
        "poll_interval": POLL_INTERVAL_SECONDS,
        "error": cache.error,
    }


# Explicit routes, not a static mount - a mount would also serve .git.
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "map.html")


@app.get("/vehicles.json")
async def vehicles_snapshot() -> FileResponse:
    """The snapshot from main.py, used for the initial render."""
    return FileResponse(BASE_DIR / "vehicles.json")
