"""
ISS Position Producer — polls Open-Notify API every N seconds.

API: http://api.open-notify.org/iss-now.json
Topic: sat.position.raw
Key: SAT-25544

Design Patterns:
- Event Sourcing : Every ISS position snapshot is an immutable event record.
- Polling Loop   : Simple periodic fetch — no webhooks available from this API.
- Retry (tenacity): Exponential back-off on transient HTTP / network errors.
"""

import asyncio
import time
from typing import Optional

import aiohttp
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .base_producer import BaseProducer
from .schemas import PositionMessage, utc_now


# ── Constants ─────────────────────────────────────────────

ISS_NORAD_ID = 25544
ISS_API_URL = "http://api.open-notify.org/iss-now.json"
METRICS_INTERVAL_S = 300   # Log metrics every 5 minutes


# ── Producer ──────────────────────────────────────────────


class ISSProducer(BaseProducer):
    """
    Polls the Open-Notify ISS position API and publishes to sat.position.raw.

    The Open-Notify API returns a bare latitude/longitude with no altitude
    or orbital parameters.  Those fields are null in the message; the
    Spark enrichment job fills them from TLE data.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        poll_interval: float = 5.0,
    ) -> None:
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic="sat.position.raw",
            producer_name="iss-producer",
        )
        self.poll_interval = poll_interval
        self._session: Optional[aiohttp.ClientSession] = None

    # ── HTTP ─────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return (or lazily create) the shared aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        reraise=False,
    )
    async def _fetch_position(self) -> Optional[dict]:
        """
        Fetch current ISS position from Open-Notify.

        Returns the parsed JSON dict on success, or None on non-200 response.
        Tenacity retries up to 3 times on connection/timeout errors.
        """
        session = await self._get_session()
        async with session.get(ISS_API_URL) as resp:
            if resp.status != 200:
                logger.warning(
                    f"[iss-producer] API returned HTTP {resp.status}"
                )
                self._api_errors += 1
                return None
            data = await resp.json()

        if data.get("message") != "success":
            logger.warning(f"[iss-producer] Unexpected API response: {data}")
            self._api_errors += 1
            return None

        return data

    # ── Processing ───────────────────────────────────────

    async def _process_and_publish(self, raw: dict) -> None:
        """Parse the Open-Notify response and publish a PositionMessage."""
        pos = raw["iss_position"]

        msg = PositionMessage(
            satellite_id=ISS_NORAD_ID,
            satellite_name="ISS",
            latitude=float(pos["latitude"]),
            longitude=float(pos["longitude"]),
            altitude_km=None,       # Not provided by Open-Notify
            timestamp=int(raw["timestamp"]),
            source="open-notify",
            ingestion_time=utc_now(),
        )

        await self.publish(msg.model_dump(), key=msg.kafka_key())
        logger.debug(
            f"[iss-producer] Published | "
            f"lat={msg.latitude:.4f} lon={msg.longitude:.4f} "
            f"ts={msg.timestamp}"
        )

    # ── Main loop ────────────────────────────────────────

    async def run(self) -> None:
        """Poll ISS position every poll_interval seconds until cancelled."""
        self._running = True
        logger.info(
            f"[iss-producer] Starting | "
            f"interval={self.poll_interval}s  url={ISS_API_URL}"
        )

        last_metrics = time.monotonic()

        try:
            while self._running:
                loop_start = time.monotonic()

                try:
                    raw = await self._fetch_position()
                    if raw:
                        await self._process_and_publish(raw)
                except Exception as exc:
                    # Tenacity exhausted retries — log and continue
                    logger.error(f"[iss-producer] Fetch failed: {exc}")
                    self._api_errors += 1

                # Periodic metrics
                if time.monotonic() - last_metrics >= METRICS_INTERVAL_S:
                    self.log_metrics()
                    last_metrics = time.monotonic()

                # Sleep for the remainder of the poll interval
                elapsed = time.monotonic() - loop_start
                sleep_time = max(0.0, self.poll_interval - elapsed)
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("[iss-producer] Cancelled")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            await self.shutdown()
