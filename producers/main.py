"""
Producer orchestrator — runs all four Kafka producers concurrently.

Usage (from project root, venv activated):
    python -m producers.main

Environment is read from .env in the project root.
Producers connect to Kafka on localhost:29092 (external listener).

Design Pattern: Fan-out / Concurrent Producers via asyncio.gather.
Each producer runs as an independent asyncio Task; a shutdown event
coordinates graceful teardown on SIGINT / Ctrl-C.
"""

import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from loguru import logger

from .donki_producer import DONKIProducer
from .iss_producer import ISSProducer
from .n2yo_producer import N2YOProducer
from .tle_producer import TLEProducer
from .base_producer import BaseProducer


# ── Environment ───────────────────────────────────────────

# Locate project root (one level above the producers/ package)
_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _env(key: str, default: str = "") -> str:
    """Read an environment variable, warning if it is missing."""
    value = os.getenv(key, default)
    if not value:
        logger.warning(f"Environment variable '{key}' is not set")
    return value


def _parse_satellites() -> list:
    """
    Parse the TRACKED_SATELLITES env var into a list of satellite dicts.

    Format: comma-separated NORAD IDs, e.g. "25544,20580,43013".
    Well-known IDs are mapped to human-readable names; unknown IDs
    get a generic "SAT-{id}" label.
    """
    KNOWN: dict = {
        25544: "ISS",
        20580: "HUBBLE",
        43013: "STARLINK-24",
        27607: "ENVISAT",
        39634: "NOAA-19",
        44713: "STARLINK-1007",
    }

    raw = _env("TRACKED_SATELLITES", "25544,20580,43013")
    satellites = []

    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            norad_id = int(part)
            satellites.append(
                {"norad_id": norad_id, "name": KNOWN.get(norad_id, f"SAT-{norad_id}")}
            )
        except ValueError:
            logger.warning(f"Ignoring invalid NORAD ID: {part!r}")

    if not satellites:
        logger.error("TRACKED_SATELLITES is empty — no satellites to track!")

    return satellites


# ── Logging ───────────────────────────────────────────────


def _configure_logging() -> None:
    """Set up loguru: coloured console + rotating file sink."""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_dir = _PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)

    logger.remove()

    # Console — human-friendly coloured output
    logger.add(
        sys.stderr,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "{message}"
        ),
        colorize=True,
    )

    # File — full DEBUG log, rotated at 50 MB, kept 7 days
    logger.add(
        str(log_dir / "producers.log"),
        level="DEBUG",
        rotation="50 MB",
        retention="7 days",
        compression="gz",
        encoding="utf-8",
    )


# ── Producer factory ──────────────────────────────────────


def _build_producers() -> List[BaseProducer]:
    """
    Instantiate all four producers from environment configuration.

    Producers connect via the *external* Kafka listener (localhost:29092)
    because they run on the host machine, not inside Docker.
    """
    bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS_EXTERNAL", "localhost:29092")
    satellites = _parse_satellites()

    return [
        ISSProducer(
            bootstrap_servers=bootstrap,
            poll_interval=float(_env("ISS_POLL_INTERVAL_SECONDS", "5")),
        ),
        N2YOProducer(
            bootstrap_servers=bootstrap,
            api_key=_env("N2YO_API_KEY"),
            satellites=satellites,
            observer_lat=float(_env("OBSERVER_LATITUDE", "36.8")),
            observer_lng=float(_env("OBSERVER_LONGITUDE", "10.18")),
            observer_alt=float(_env("OBSERVER_ALTITUDE", "0")),
            poll_interval=float(_env("N2YO_POLL_INTERVAL_SECONDS", "15")),
        ),
        DONKIProducer(
            bootstrap_servers=bootstrap,
            api_key=_env("NASA_API_KEY", "DEMO_KEY"),
            poll_interval=float(_env("DONKI_POLL_INTERVAL_SECONDS", "300")),
            lookback_days=7,
        ),
        TLEProducer(
            bootstrap_servers=bootstrap,
            satellites=satellites,
            fetch_interval=float(_env("TLE_FETCH_INTERVAL_SECONDS", "3600")),
        ),
    ]


# ── Shutdown coordination ─────────────────────────────────

_shutdown_event: asyncio.Event


def _request_shutdown(sig_name: str) -> None:
    logger.info(f"Received {sig_name} — initiating graceful shutdown...")
    _shutdown_event.set()


# ── Entry point ───────────────────────────────────────────


async def _run() -> None:
    global _shutdown_event
    _shutdown_event = asyncio.Event()

    _configure_logging()

    logger.info("=" * 60)
    logger.info("  Satellite Tracking Platform — Kafka Producers")
    logger.info("=" * 60)

    producers = _build_producers()
    logger.info(
        f"Launching {len(producers)} producers: "
        f"{[type(p).__name__ for p in producers]}"
    )

    # Register OS signals for graceful shutdown.
    # add_signal_handler is not supported on Windows for SIGTERM,
    # so we catch NotImplementedError and rely on KeyboardInterrupt instead.
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig.name: _request_shutdown(s)
            )
        except (NotImplementedError, OSError):
            pass  # Windows — Ctrl-C raises KeyboardInterrupt, handled below

    # Create one asyncio Task per producer
    producer_tasks = [
        asyncio.create_task(p.run(), name=type(p).__name__)
        for p in producers
    ]

    # Also wait on the shutdown event so we can exit cleanly on signal
    shutdown_task = asyncio.create_task(_shutdown_event.wait(), name="shutdown-watcher")

    all_tasks = producer_tasks + [shutdown_task]

    # Block until either a producer crashes or shutdown is requested
    done, pending = await asyncio.wait(
        all_tasks, return_when=asyncio.FIRST_COMPLETED
    )

    # Report any producer that exited unexpectedly
    for task in done:
        if task is not shutdown_task and not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error(
                    f"Producer '{task.get_name()}' crashed: {exc}"
                )

    # Cancel every remaining task
    logger.info("Cancelling remaining producer tasks...")
    for task in pending:
        task.cancel()

    # Wait for cancellations to complete (CancelledError is suppressed)
    await asyncio.gather(*pending, return_exceptions=True)

    # Each producer's run() calls shutdown() in its finally block,
    # but call it again here to be safe (shutdown() is idempotent via _running flag)
    logger.info("All producers stopped.")


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        # Ctrl-C on Windows when add_signal_handler is not available
        logger.info("Interrupted by user (KeyboardInterrupt)")
    logger.info("Producers exited cleanly.")


if __name__ == "__main__":
    main()
