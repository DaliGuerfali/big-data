"""
TLE (Two-Line Element) Bulk Fetcher Producer.

API: https://tle.ivanstanojevic.me/api/tle/{norad_id}
Topic: sat.tle.raw
Key: TLE-{norad_id}

Design Patterns:
- Scheduled Batch : Fetches infrequently (hourly) — TLEs are updated once per day.
- Change Detection: Compares TLE line1 against the previous fetch; only publishes
                    when the element set has actually changed.
- Cold Path Data  : TLE messages feed the MapReduce drift analysis batch job,
                    not the real-time stream — demonstrating Lambda Architecture's
                    cold/batch layer.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .base_producer import BaseProducer
from .schemas import TLEMessage, utc_now


# ── Constants ─────────────────────────────────────────────

TLE_BASE_URL = "https://tle.ivanstanojevic.me/api/tle"


# ── TLE Epoch Parser ──────────────────────────────────────


def _parse_tle_epoch(line1: str) -> Optional[str]:
    """
    Extract the epoch from TLE line 1 and return an ISO-8601 UTC string.

    TLE line1 format (0-indexed columns):
        18-19  : 2-digit year  (e.g. "24" → 2024)
        20-31  : Day of year + fractional day  (e.g. "015.45678901")

    Years < 57 are assumed 2000s; years >= 57 are 1900s (SGP4 convention).
    Returns None if parsing fails so callers can log and continue.
    """
    try:
        epoch_str = line1[18:32].strip()
        year_2d = int(epoch_str[:2])
        day_frac = float(epoch_str[2:])

        full_year = 2000 + year_2d if year_2d < 57 else 1900 + year_2d

        # day_frac is 1-based (day 1 = Jan 1)
        epoch_dt = datetime(full_year, 1, 1, tzinfo=timezone.utc) + timedelta(
            days=day_frac - 1
        )
        return epoch_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    except Exception:
        return None


# ── Producer ──────────────────────────────────────────────


class TLEProducer(BaseProducer):
    """
    Fetches Two-Line Element sets for all tracked satellites and publishes
    changed TLEs to sat.tle.raw.

    TLE data is updated by NORAD/Space-Track once per day.  We poll hourly
    so updates are picked up within an hour of release.  Change detection
    (comparing line1 which embeds the epoch) prevents flooding the topic with
    identical data.

    The published messages feed:
      - MapReduce batch job: weekly TLE drift analysis
      - Spark batch job: historical orbit reconstruction
    """

    def __init__(
        self,
        bootstrap_servers: str,
        satellites: List[Dict[str, Any]],
        fetch_interval: float = 3600.0,
    ) -> None:
        """
        Args:
            bootstrap_servers: Kafka broker (external: localhost:29092).
            satellites:        List of {"norad_id": int, "name": str}.
            fetch_interval:    Seconds between full fetch cycles (default 1 hr).
        """
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic="sat.tle.raw",
            producer_name="tle-producer",
        )
        self.satellites = satellites
        self.fetch_interval = fetch_interval
        self._session: Optional[aiohttp.ClientSession] = None

        # Change detection: norad_id → last published tle_line1
        self._last_line1: Dict[int, str] = {}

    # ── HTTP ─────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=20, connect=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        reraise=False,
    )
    async def _fetch_tle(self, norad_id: int) -> Optional[dict]:
        """
        Fetch the latest TLE for a single satellite from the TLE API.

        Returns the parsed JSON dict, or None on 404 / non-200 response.
        Retries up to 4 times with exponential backoff.
        """
        session = await self._get_session()
        url = f"{TLE_BASE_URL}/{norad_id}"

        async with session.get(url) as resp:
            if resp.status == 404:
                logger.warning(
                    f"[tle-producer] NORAD {norad_id} not found in TLE API"
                )
                return None

            if resp.status != 200:
                logger.warning(
                    f"[tle-producer] HTTP {resp.status} for NORAD {norad_id}"
                )
                self._api_errors += 1
                return None

            return await resp.json()

    # ── Processing ───────────────────────────────────────

    async def _process_and_publish(
        self, norad_id: int, default_name: str, raw: dict
    ) -> bool:
        """
        Validate the TLE response and publish if the epoch has changed.

        Returns True if a new message was published, False if skipped.
        """
        line1 = (raw.get("line1") or "").strip()
        line2 = (raw.get("line2") or "").strip()
        sat_name = (raw.get("name") or default_name).strip()

        if not line1 or not line2:
            logger.warning(
                f"[tle-producer] Empty TLE lines for {sat_name} (NORAD {norad_id})"
            )
            return False

        # Skip if TLE hasn't changed since last successful publish
        if self._last_line1.get(norad_id) == line1:
            logger.debug(
                f"[tle-producer] TLE unchanged for {sat_name} "
                f"(NORAD {norad_id}) — skipping"
            )
            return False

        self._last_line1[norad_id] = line1
        epoch = _parse_tle_epoch(line1)

        msg = TLEMessage(
            satellite_id=norad_id,
            satellite_name=sat_name,
            tle_line1=line1,
            tle_line2=line2,
            epoch=epoch,
            source="tle-api",
            ingestion_time=utc_now(),
        )

        await self.publish(msg.model_dump(), key=msg.kafka_key())
        logger.info(
            f"[tle-producer] Published TLE | "
            f"satellite={sat_name}  NORAD={norad_id}  epoch={epoch}"
        )
        return True

    # ── Fetch cycle ──────────────────────────────────────

    async def _fetch_all(self) -> None:
        """
        Iterate through all tracked satellites and publish changed TLEs.

        A 2-second pause between individual fetches keeps us respectful
        of the free TLE API's resources.
        """
        published = 0
        skipped = 0

        for sat in self.satellites:
            if not self._running:
                break

            norad_id = sat["norad_id"]
            name = sat["name"]

            try:
                raw = await self._fetch_tle(norad_id)
                if raw:
                    changed = await self._process_and_publish(norad_id, name, raw)
                    if changed:
                        published += 1
                    else:
                        skipped += 1
            except Exception as exc:
                logger.error(
                    f"[tle-producer] Error fetching {name} "
                    f"(NORAD {norad_id}): {exc}"
                )
                self._api_errors += 1

            # Polite delay between individual satellite fetches
            await asyncio.sleep(2)

        logger.info(
            f"[tle-producer] Fetch cycle done | "
            f"published={published}  unchanged={skipped}  "
            f"total_satellites={len(self.satellites)}"
        )

    # ── Main loop ────────────────────────────────────────

    async def run(self) -> None:
        """
        Fetch TLE on startup then sleep fetch_interval seconds between cycles.

        Fetching immediately on startup means the rest of the pipeline has
        fresh TLE data before any Spark streaming jobs try to use it.
        """
        self._running = True
        logger.info(
            f"[tle-producer] Starting | "
            f"interval={self.fetch_interval}s ({self.fetch_interval/3600:.1f}h)  "
            f"satellites={[s['name'] for s in self.satellites]}"
        )

        try:
            while self._running:
                loop_start = time.monotonic()

                await self._fetch_all()
                self.log_metrics()

                elapsed = time.monotonic() - loop_start
                sleep_time = max(0.0, self.fetch_interval - elapsed)
                next_run = datetime.now(timezone.utc) + timedelta(seconds=sleep_time)

                logger.info(
                    f"[tle-producer] Next fetch at "
                    f"{next_run.strftime('%H:%M:%S UTC')} "
                    f"(in {sleep_time/60:.1f} min)"
                )
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("[tle-producer] Cancelled")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            await self.shutdown()
