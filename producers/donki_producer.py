"""
NASA DONKI Space Weather Event Producer.

API: https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/{EventType}
Topic: sat.events.raw
Key: EVT-{event_type}

Design Patterns:
- Deduplication    : In-memory set of seen event IDs prevents republishing events
                     that persist across lookback windows.
- Lookback Window  : Queries the last N days on every poll to catch late-arriving
                     or revised events from NASA's modelling pipeline.
- Event Sourcing   : Each space weather event is an immutable, timestamped record
                     that feeds both stream and batch processing.
- Severity Mapping : Type-specific fields (flare class, Kp index, CME speed) are
                     normalised into a single low|moderate|high|extreme scale.
"""

import asyncio
import hashlib
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import aiohttp
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .base_producer import BaseProducer
from .schemas import SpaceWeatherEvent, utc_now


# ── Constants ─────────────────────────────────────────────

DONKI_BASE_URL = "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get"

# Ordered list so CME and FLR (most impactful) are fetched first
EVENT_TYPES = ["CME", "FLR", "GST", "SEP", "IPS", "HSS"]

# Per-event-type config: DONKI endpoint name and the field that holds the unique ID.
# IPS has no stable ID field; we derive one from a content hash.
EVENT_CONFIG: Dict[str, Dict[str, Any]] = {
    "CME": {"endpoint": "CME", "id_field": "activityID"},
    "FLR": {"endpoint": "FLR", "id_field": "flrID"},
    "GST": {"endpoint": "GST", "id_field": "gstID"},
    "SEP": {"endpoint": "SEP", "id_field": "sepID"},
    "IPS": {"endpoint": "IPS", "id_field": None},
    "HSS": {"endpoint": "HSS", "id_field": "hssID"},
}

METRICS_INTERVAL_S = 600   # 10 minutes


# ── Helpers ───────────────────────────────────────────────


def _extract_event_id(event_type: str, event: dict) -> str:
    """
    Return the canonical unique ID for this event.

    Uses the DONKI-assigned ID field when present; falls back to a
    deterministic MD5 hash of (type, startTime, beginTime) for event
    types that lack a stable ID (IPS).
    """
    id_field = EVENT_CONFIG[event_type]["id_field"]
    if id_field and event.get(id_field):
        return str(event[id_field])

    # Derive a stable ID from immutable event attributes
    raw = f"{event_type}|{event.get('startTime', '')}|{event.get('beginTime', '')}"
    digest = hashlib.md5(raw.encode()).hexdigest()[:12]
    return f"{event_type}-{digest}"


def _extract_instruments(event: dict) -> List[str]:
    """Flatten the instruments list to display names."""
    result = []
    for inst in event.get("instruments") or []:
        name = inst.get("displayName") or inst.get("instrumentType")
        if name:
            result.append(str(name))
    return result


def _extract_linked_events(event: dict) -> List[str]:
    """Return list of linked activityIDs (cross-event references from DONKI)."""
    result = []
    for le in event.get("linkedEvents") or []:
        aid = le.get("activityID")
        if aid:
            result.append(str(aid))
    return result


def _classify_severity(event_type: str, event: dict) -> str:
    """
    Map type-specific intensity metrics to a unified severity scale.

    FLR  → GOES X-ray class  (A/B = low, C = moderate, M = high, X = extreme)
    GST  → Kp index           (< 3 = low, 3-4 = moderate, 5-6 = high, >= 7 = extreme)
    CME  → Speed (km/s)       (< 500 = low, 500-999 = moderate, 1000-1999 = high, >= 2000 = extreme)
    Others → unknown (no standard intensity metric from DONKI)
    """
    if event_type == "FLR":
        cls = (event.get("classType") or "").upper().strip()
        if cls.startswith("X"):
            return "extreme"
        if cls.startswith("M"):
            return "high"
        if cls.startswith("C"):
            return "moderate"
        return "low"

    if event_type == "GST":
        # allKpIndex is a list of time-series observations
        observations = event.get("allKpIndex") or []
        kp = max((float(o.get("kpIndex", 0)) for o in observations), default=0.0)
        if kp >= 7:
            return "extreme"
        if kp >= 5:
            return "high"
        if kp >= 3:
            return "moderate"
        return "low"

    if event_type == "CME":
        # cmeAnalyses contains modelled trajectories; take the fastest
        speed = 0.0
        for analysis in event.get("cmeAnalyses") or []:
            speed = max(speed, float(analysis.get("speed") or 0))
        if speed >= 2000:
            return "extreme"
        if speed >= 1000:
            return "high"
        if speed >= 500:
            return "moderate"
        return "low"

    return "unknown"


# ── Producer ──────────────────────────────────────────────


class DONKIProducer(BaseProducer):
    """
    Polls NASA DONKI for all space weather event types and publishes
    new events to sat.events.raw.

    On each poll cycle all six event types are queried over a rolling
    lookback window.  Already-seen event IDs are tracked in memory to
    avoid duplicate messages.  The lookback window intentionally overlaps
    previous cycles so late-revised events are captured.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        api_key: str = "DEMO_KEY",
        poll_interval: float = 300.0,
        lookback_days: int = 7,
    ) -> None:
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic="sat.events.raw",
            producer_name="donki-producer",
        )
        self.api_key = api_key
        self.poll_interval = poll_interval
        self.lookback_days = lookback_days
        self._session: Optional[aiohttp.ClientSession] = None
        self._seen_ids: Set[str] = set()    # Deduplication across cycles

        logger.info(
            f"[donki-producer] Config | "
            f"interval={poll_interval}s  lookback={lookback_days}d  "
            f"types={EVENT_TYPES}"
        )

    # ── HTTP ─────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    def _date_range(self) -> tuple[str, str]:
        """Return (start_date, end_date) strings for the lookback window."""
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=self.lookback_days)
        fmt = "%Y-%m-%d"
        return start.strftime(fmt), end.strftime(fmt)

    def _build_url(self, event_type: str) -> str:
        start, end = self._date_range()
        endpoint = EVENT_CONFIG[event_type]["endpoint"]
        url = f"{DONKI_BASE_URL}/{endpoint}?startDate={start}&endDate={end}"
        # Only append api_key if it is a real personal key
        if self.api_key and self.api_key.upper() != "DEMO_KEY":
            url += f"&api_key={self.api_key}"
        return url

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        reraise=False,
    )
    async def _fetch_events(self, event_type: str) -> List[dict]:
        """
        Fetch all events of `event_type` within the lookback window.

        DONKI returns a JSON array, or an empty list when no events occurred.
        Occasionally it returns HTTP 200 with 'text/plain' content-type for
        empty responses; content_type=None handles that gracefully.
        """
        session = await self._get_session()
        url = self._build_url(event_type)

        async with session.get(url) as resp:
            if resp.status == 429:
                logger.warning(
                    f"[donki-producer] Rate limited for {event_type} — waiting 30s"
                )
                self._api_errors += 1
                await asyncio.sleep(30)
                return []

            if resp.status != 200:
                logger.warning(
                    f"[donki-producer] HTTP {resp.status} for {event_type}"
                )
                self._api_errors += 1
                return []

            # content_type=None bypasses the MIME check so text/plain works too
            data = await resp.json(content_type=None)

        if not isinstance(data, list):
            # NASA sometimes returns {"error": "..."} on bad date ranges
            logger.debug(f"[donki-producer] Non-list response for {event_type}: {data}")
            return []

        return data

    # ── Processing ───────────────────────────────────────

    async def _process_events(self, event_type: str, events: List[dict]) -> int:
        """
        Publish events that haven't been seen before.

        Returns the count of newly published events.
        """
        published = 0

        for event in events:
            event_id = _extract_event_id(event_type, event)

            if event_id in self._seen_ids:
                continue                        # Already published — skip

            self._seen_ids.add(event_id)

            msg = SpaceWeatherEvent(
                event_id=event_id,
                event_type=event_type,
                start_time=event.get("startTime") or event.get("beginTime"),
                peak_time=event.get("peakTime"),
                end_time=event.get("endTime"),
                source_location=(
                    event.get("sourceLocation")
                    or str(event.get("activeRegionNum") or "")
                    or None
                ),
                instruments=_extract_instruments(event),
                linked_events=_extract_linked_events(event),
                severity=_classify_severity(event_type, event),
                note=event.get("note") or event.get("catalog"),
                raw_data=event,
                source="donki",
                ingestion_time=utc_now(),
            )

            await self.publish(msg.model_dump(), key=msg.kafka_key())
            published += 1

            logger.info(
                f"[donki-producer] Published {event_type} | "
                f"id={event_id}  severity={msg.severity}  "
                f"start={msg.start_time}"
            )

        return published

    async def _poll_cycle(self) -> None:
        """Run one full poll cycle over all event types."""
        total_new = 0

        for event_type in EVENT_TYPES:
            if not self._running:
                break
            try:
                events = await self._fetch_events(event_type)
                new_count = await self._process_events(event_type, events)
                total_new += new_count

                logger.debug(
                    f"[donki-producer] {event_type}: "
                    f"fetched={len(events)}  new={new_count}  "
                    f"dedup_cache={len(self._seen_ids)}"
                )

                # Small pause between event-type requests to be polite to NASA
                await asyncio.sleep(1)

            except Exception as exc:
                logger.error(
                    f"[donki-producer] Unhandled error for {event_type}: {exc}"
                )
                self._api_errors += 1

        if total_new:
            logger.info(
                f"[donki-producer] Cycle complete — published {total_new} new events"
            )
        else:
            logger.debug("[donki-producer] Cycle complete — no new events")

    # ── Main loop ────────────────────────────────────────

    async def run(self) -> None:
        """Poll DONKI every poll_interval seconds until cancelled."""
        self._running = True
        logger.info(
            f"[donki-producer] Starting | "
            f"interval={self.poll_interval}s  lookback={self.lookback_days}d"
        )

        last_metrics = time.monotonic()

        try:
            while self._running:
                loop_start = time.monotonic()

                await self._poll_cycle()

                if time.monotonic() - last_metrics >= METRICS_INTERVAL_S:
                    self.log_metrics()
                    last_metrics = time.monotonic()

                elapsed = time.monotonic() - loop_start
                sleep_time = max(0.0, self.poll_interval - elapsed)
                logger.debug(
                    f"[donki-producer] Sleeping {sleep_time:.0f}s until next cycle"
                )
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("[donki-producer] Cancelled")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            await self.shutdown()
