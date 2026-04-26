"""
Satellite Tracking — Kafka Producers package.

Producers push data from three external APIs into six Kafka topics:

    ISS Producer   → sat.position.raw   (Open-Notify, every 5 s)
    N2YO Producer  → sat.position.raw   (N2YO, every 15 s, multi-satellite)
    DONKI Producer → sat.events.raw     (NASA DONKI, every 5 min)
    TLE Producer   → sat.tle.raw        (TLE API, every 1 hr)

Run all producers concurrently:
    python -m producers.main
"""

from .base_producer import BaseProducer, TokenBucket
from .schemas import PositionMessage, SpaceWeatherEvent, TLEMessage, utc_now
from .iss_producer import ISSProducer
from .n2yo_producer import N2YOProducer
from .donki_producer import DONKIProducer
from .tle_producer import TLEProducer

__all__ = [
    "BaseProducer",
    "TokenBucket",
    "PositionMessage",
    "SpaceWeatherEvent",
    "TLEMessage",
    "utc_now",
    "ISSProducer",
    "N2YOProducer",
    "DONKIProducer",
    "TLEProducer",
]
