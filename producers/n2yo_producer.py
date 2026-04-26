"""
N2YO Multi-Satellite Position Producer.

API: https://api.n2yo.com/rest/v1/satellite/positions/{id}/{lat}/{lng}/{alt}/1
Topic: sat.position.raw
Key: SAT-{norad_id}

Design Patterns:
- Token Bucket   : Stays within N2YO's 1000 req/hr limit with a smooth rate limiter.
- Round Robin    : Iterates satellite list sequentially for fair, predictable polling.
- Retry          : Exponential back-off on transient HTTP errors.
- Schema Unification: Maps N2YO's richer response onto the shared PositionMessage schema.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .base_producer import BaseProducer, TokenBucket
from .schemas import PositionMessage, utc_now


# ── Constants ─────────────────────────────────────────────

N2YO_BASE_URL = "https://api.n2yo.com/rest/v1/satellite"
METRICS_INTERVAL_S = 300


# ── Producer ──────────────────────────────────────────────


class N2YOProducer(BaseProducer):
    """
    Polls N2YO for the current position of each tracked satellite and
    publishes to sat.position.raw.

    N2YO returns richer orbital data than Open-Notify:
    altitude, azimuth, elevation, right ascension, declination,
    and an eclipsed flag (in Earth's shadow).

    Rate-limiting strategy:
      - N2YO allows 1000 req/hr for the /positions endpoint.
      - We configure a TokenBucket at 900 req/hr (10% safety margin).
      - With 3 satellites and a 15s poll interval, we consume ~720 req/hr —
        well within limits even as the satellite list grows.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        api_key: str,
        satellites: List[Dict[str, Any]],
        observer_lat: float,
        observer_lng: float,
        observer_alt: float,
        poll_interval: float = 15.0,
    ) -> None:
        """
        Args:
            bootstrap_servers: Kafka broker address (external: localhost:29092).
            api_key:           N2YO API key from .env.
            satellites:        List of {"norad_id": int, "name": str} dicts.
            observer_lat/lng/alt: Observer ground station coordinates (Tunisia).
            poll_interval:     Seconds between full satellite-list poll cycles.
        """
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic="sat.position.raw",
            producer_name="n2yo-producer",
        )
        self.api_key = api_key
        self.satellites = satellites
        self.observer_lat = observer_lat
        self.observer_lng = observer_lng
        self.observer_alt = observer_alt
        self.poll_interval = poll_interval
        self._session: Optional[aiohttp.ClientSession] = None

        # 900 req/hr sustained, burst of 5
        self._rate_limiter = TokenBucket(rate_per_hour=900.0, burst_size=5.0)

        logger.info(
            f"[n2yo-producer] Tracking {len(satellites)} satellites: "
            f"{[s['name'] for s in satellites]} | "
            f"observer=({observer_lat}, {observer_lng})"
        )

    # ── HTTP ─────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    def _position_url(self, norad_id: int) -> str:
        """Build the N2YO /positions URL for a single current-position request."""
        return (
            f"{N2YO_BASE_URL}/positions/{norad_id}/"
            f"{self.observer_lat}/{self.observer_lng}/{self.observer_alt}/1"
            f"?apiKey={self.api_key}"
        )

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=30),
        reraise=False,
    )
    async def _fetch_position(
        self, norad_id: int, name: str
    ) -> Optional[dict]:
        """
        Acquire a rate-limit token, then fetch the current position.

        Returns a dict with keys 'info' and 'position', or None on failure.
        """
        # Block here until the token bucket allows this request
        await self._rate_limiter.acquire()

        session = await self._get_session()
        url = self._position_url(norad_id)

        async with session.get(url) as resp:
            if resp.status == 429:
                logger.warning(
                    f"[n2yo-producer] Rate limited by N2YO for {name} "
                    f"(NORAD {norad_id}) — backing off 60s"
                )
                self._api_errors += 1
                await asyncio.sleep(60)
                return None

            if resp.status != 200:
                logger.warning(
                    f"[n2yo-producer] HTTP {resp.status} for {name} "
                    f"(NORAD {norad_id})"
                )
                self._api_errors += 1
                return None

            data = await resp.json()

        positions = data.get("positions", [])
        if not positions:
            logger.warning(
                f"[n2yo-producer] No positions returned for {name} "
                f"(NORAD {norad_id}) — satellite may be below horizon"
            )
            return None

        return {"info": data.get("info", {}), "position": positions[0]}

    # ── Processing ───────────────────────────────────────

    async def _process_and_publish(
        self, norad_id: int, name: str, raw: dict
    ) -> None:
        """Map the N2YO response onto PositionMessage and publish."""
        info = raw["info"]
        pos = raw["position"]

        # N2YO returns the official satellite name in info.satname
        sat_name = str(info.get("satname", name)).strip()

        msg = PositionMessage(
            satellite_id=norad_id,
            satellite_name=sat_name,
            latitude=float(pos["satlatitude"]),
            longitude=float(pos["satlongitude"]),
            altitude_km=float(pos["sataltitude"]) if pos.get("sataltitude") else None,
            azimuth=float(pos["azimuth"]) if pos.get("azimuth") is not None else None,
            elevation=(
                float(pos["elevation"]) if pos.get("elevation") is not None else None
            ),
            ra=float(pos["ra"]) if pos.get("ra") is not None else None,
            dec=float(pos["dec"]) if pos.get("dec") is not None else None,
            eclipsed=bool(pos.get("eclipsed", False)),
            timestamp=int(pos["timestamp"]),
            source="n2yo",
            ingestion_time=utc_now(),
        )

        await self.publish(msg.model_dump(), key=msg.kafka_key())
        logger.debug(
            f"[n2yo-producer] Published {sat_name} | "
            f"lat={msg.latitude:.4f} lon={msg.longitude:.4f} "
            f"alt={msg.altitude_km:.1f}km eclipsed={msg.eclipsed}"
        )

    # ── Poll cycle ───────────────────────────────────────

    async def _poll_all_satellites(self) -> None:
        """
        Fetch and publish positions for all tracked satellites sequentially.

        Sequential (not concurrent) so the token bucket paces requests smoothly
        rather than firing all requests in a burst at the start of each cycle.
        """
        for sat in self.satellites:
            if not self._running:
                break
            norad_id = sat["norad_id"]
            name = sat["name"]
            try:
                raw = await self._fetch_position(norad_id, name)
                if raw:
                    await self._process_and_publish(norad_id, name, raw)
            except Exception as exc:
                logger.error(
                    f"[n2yo-producer] Unhandled error for {name} "
                    f"(NORAD {norad_id}): {exc}"
                )
                self._api_errors += 1

    # ── Main loop ────────────────────────────────────────

    async def run(self) -> None:
        """Poll all satellites every poll_interval seconds until cancelled."""
        self._running = True
        logger.info(
            f"[n2yo-producer] Starting | "
            f"interval={self.poll_interval}s  satellites={len(self.satellites)}"
        )

        last_metrics = time.monotonic()

        try:
            while self._running:
                loop_start = time.monotonic()

                await self._poll_all_satellites()

                if time.monotonic() - last_metrics >= METRICS_INTERVAL_S:
                    self.log_metrics()
                    last_metrics = time.monotonic()

                elapsed = time.monotonic() - loop_start
                sleep_time = max(0.0, self.poll_interval - elapsed)
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("[n2yo-producer] Cancelled")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            await self.shutdown()
