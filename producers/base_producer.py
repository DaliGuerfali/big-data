"""
Base Kafka producer with rate limiting, retry logic, and metrics.

Design Patterns demonstrated:
- Template Method   : run() defines algorithm skeleton; subclasses fill _fetch / _publish steps.
- Token Bucket      : Smooth API rate limiting for external calls.
- Circuit Breaker   : Via tenacity exponential backoff on repeated failures.
- Metrics Collection: Per-producer counters for observability.
"""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from confluent_kafka import Producer
from loguru import logger


# ── Token Bucket Rate Limiter ─────────────────────────────


class TokenBucket:
    """
    Thread-safe async token bucket for API rate limiting.

    Allows burst traffic up to `burst_size` tokens, then smooths
    requests to `rate_per_hour` over time. Uses asyncio.Lock so it is
    safe to use inside the event loop.

    Example:
        # Allow 1000 requests/hr with burst of 10
        limiter = TokenBucket(rate_per_hour=1000, burst_size=10)
        await limiter.acquire()   # blocks until a token is available
    """

    def __init__(self, rate_per_hour: float, burst_size: Optional[float] = None):
        """
        Args:
            rate_per_hour: Sustained maximum requests per hour.
            burst_size:    Maximum token reservoir. Defaults to 5% of hourly
                           rate (capped at 20) to allow small bursts.
        """
        self.rate: float = rate_per_hour / 3600.0          # tokens / second
        self.capacity: float = burst_size or min(rate_per_hour * 0.05, 20.0)
        self.tokens: float = self.capacity
        self._last_refill: float = time.monotonic()
        self._lock: asyncio.Lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until one token is available, then consume it."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self._last_refill = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                wait_time = (1.0 - self.tokens) / self.rate
                self.tokens = 0.0
                await asyncio.sleep(wait_time)


# ── Base Producer ─────────────────────────────────────────


class BaseProducer(ABC):
    """
    Abstract base class for all Kafka producers.

    Subclasses must implement run(), which is the main polling loop.
    This class handles:
      - Kafka Producer initialization and configuration
      - Async-safe publish() via ThreadPoolExecutor
      - Delivery callbacks and metrics counters
      - Graceful shutdown with flush
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        producer_name: str,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_name = producer_name
        self._running: bool = False

        # Thread pool for blocking confluent-kafka calls (flush)
        self._executor = ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix=f"kafka-{producer_name}",
        )

        # Metrics
        self._messages_sent: int = 0
        self._messages_failed: int = 0
        self._api_errors: int = 0
        self._start_time: float = time.monotonic()

        # Confluent Kafka producer — thread-safe, non-blocking produce()
        self._kafka = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": f"satellite-tracker-{producer_name}",
                "acks": "1",                        # Leader acknowledgment
                "retries": 3,
                "retry.backoff.ms": 300,
                "compression.type": "snappy",
                "linger.ms": 10,                    # Small batching window
                "batch.num.messages": 50,
                "queue.buffering.max.messages": 10000,
                "socket.timeout.ms": 10000,
            }
        )

        logger.info(
            f"[{self.producer_name}] Initialized | "
            f"broker={bootstrap_servers} topic={topic}"
        )

    # ── Kafka I/O ────────────────────────────────────────

    def _on_delivery(self, err, msg) -> None:
        """
        Delivery callback invoked by librdkafka in the background thread.
        Updates sent/failed counters and logs at appropriate levels.
        """
        if err:
            logger.warning(
                f"[{self.producer_name}] Delivery FAILED | "
                f"error={err} partition={msg.partition()}"
            )
            self._messages_failed += 1
        else:
            self._messages_sent += 1
            logger.debug(
                f"[{self.producer_name}] Delivered | "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    async def publish(self, message: dict, key: Optional[str] = None) -> bool:
        """
        Serialize `message` as JSON and enqueue it to Kafka.

        The confluent-kafka produce() call is non-blocking (adds to internal
        buffer), but can raise BufferError when the internal queue is full.
        We run it in an executor so it never blocks the event loop.

        Returns True if enqueued successfully, False on error.
        """
        loop = asyncio.get_event_loop()

        try:
            payload = json.dumps(message, default=str, ensure_ascii=False).encode(
                "utf-8"
            )
            key_bytes = key.encode("utf-8") if key else None

            def _produce() -> None:
                self._kafka.produce(
                    self.topic,
                    value=payload,
                    key=key_bytes,
                    on_delivery=self._on_delivery,
                )
                # Poll with timeout=0 to trigger pending delivery callbacks
                # without blocking. Actual flushing happens asynchronously.
                self._kafka.poll(0)

            await loop.run_in_executor(self._executor, _produce)
            return True

        except BufferError:
            # Internal queue full — do a partial flush and report failure
            logger.warning(
                f"[{self.producer_name}] Kafka buffer full — doing partial flush"
            )
            await loop.run_in_executor(
                self._executor, lambda: self._kafka.flush(5)
            )
            return False

        except Exception as exc:
            logger.error(f"[{self.producer_name}] Publish error: {exc}")
            self._messages_failed += 1
            return False

    async def _flush(self, timeout: float = 10.0) -> None:
        """Flush all enqueued messages. Blocks up to `timeout` seconds."""
        loop = asyncio.get_event_loop()
        remaining = await loop.run_in_executor(
            self._executor, lambda: self._kafka.flush(timeout)
        )
        if remaining > 0:
            logger.warning(
                f"[{self.producer_name}] {remaining} messages not flushed "
                f"within {timeout}s"
            )

    # ── Metrics ──────────────────────────────────────────

    def log_metrics(self) -> None:
        """Log current throughput and error counts."""
        elapsed = max(time.monotonic() - self._start_time, 1.0)
        rate = self._messages_sent / elapsed
        logger.info(
            f"[{self.producer_name}] METRICS | "
            f"sent={self._messages_sent} failed={self._messages_failed} "
            f"api_errors={self._api_errors} rate={rate:.2f} msg/s "
            f"uptime={elapsed:.0f}s"
        )

    # ── Lifecycle ────────────────────────────────────────

    async def shutdown(self) -> None:
        """Flush pending messages, log final metrics, and shut down cleanly."""
        self._running = False
        logger.info(f"[{self.producer_name}] Shutting down — flushing messages...")
        await self._flush()
        self._executor.shutdown(wait=True, cancel_futures=False)
        self.log_metrics()
        logger.info(f"[{self.producer_name}] Shutdown complete")

    @abstractmethod
    async def run(self) -> None:
        """
        Main producer loop.

        Implementations should:
        1. Set self._running = True
        2. Loop while self._running
        3. Call publish() with fetched data
        4. Sleep for the configured interval
        5. Call shutdown() in a finally block
        """
        raise NotImplementedError
