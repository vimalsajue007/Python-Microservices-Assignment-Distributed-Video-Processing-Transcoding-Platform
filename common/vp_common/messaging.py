"""Message broker abstraction.

Two implementations share one interface:

* :class:`RabbitBroker`  - production implementation on top of ``pika``.
* :class:`InMemoryBroker` - deterministic, synchronous broker used by the test
  suite. It reproduces the delivery semantics we rely on (fan-out routing,
  redelivery, dead-lettering) so service-to-service flows can be tested in-process.

Handler contract (identical for both brokers):

* return normally            -> message acknowledged
* raise :class:`RejectMessage` -> message dead-lettered immediately (poison message)
* raise any other exception   -> redelivered with a delay, up to
  ``max_redeliveries`` times, then dead-lettered
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

import pika
from pika.exceptions import AMQPError

from .topology import ALL_QUEUES, EX_DLX, EX_RETRY, QueueSpec, declare_topology

log = logging.getLogger("vp.messaging")


class BrokerError(Exception):
    """The broker is unreachable or refused a publish."""


class RejectMessage(Exception):
    """Raised by a handler to dead-letter a message without retrying."""


@dataclass
class Message:
    routing_key: str
    body: dict[str, Any]
    headers: dict[str, Any] = field(default_factory=dict)
    message_id: str | None = None
    redelivery_count: int = 0


Handler = Callable[[Message], None]


class Broker(Protocol):
    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None: ...

    def consume(self, spec: QueueSpec, handler: Handler, *, prefetch: int = 1) -> None: ...

    def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# RabbitMQ implementation
# --------------------------------------------------------------------------- #
class RabbitBroker:
    def __init__(
        self,
        url: str,
        *,
        max_redeliveries: int = 3,
        retry_delay_ms: int = 5000,
        publish_retries: int = 3,
    ):
        self._params = pika.URLParameters(url)
        self._params.heartbeat = 30
        self._params.blocked_connection_timeout = 60
        self._params.connection_attempts = 1
        self._max_redeliveries = max_redeliveries
        self._retry_delay_ms = retry_delay_ms
        self._publish_retries = publish_retries
        self._lock = threading.RLock()
        self._conn: pika.BlockingConnection | None = None
        self._channel = None
        self._consumers: list[_RabbitConsumer] = []

    # -- publishing ---------------------------------------------------------
    def _ensure_channel(self):
        if self._conn is None or self._conn.is_closed or self._channel is None or self._channel.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.confirm_delivery()
            declare_topology(self._channel)
        return self._channel

    def _reset(self) -> None:
        try:
            if self._conn is not None and self._conn.is_open:
                self._conn.close()
        except Exception:  # noqa: BLE001
            pass
        self._conn = None
        self._channel = None

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: dict[str, Any],
        *,
        message_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        payload = json.dumps(body, default=str, separators=(",", ":")).encode()
        props = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=message_id or str(uuid.uuid4()),
            headers=headers or {},
            timestamp=int(time.time()),
        )
        last: Exception | None = None
        for attempt in range(self._publish_retries):
            try:
                with self._lock:
                    channel = self._ensure_channel()
                    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=payload, properties=props)
                return
            except (AMQPError, OSError) as exc:
                last = exc
                log.warning("publish to %s/%s failed (attempt %d): %s", exchange, routing_key, attempt + 1, exc)
                with self._lock:
                    self._reset()
                time.sleep(0.3 * (attempt + 1))
        raise BrokerError(f"could not publish {exchange}/{routing_key}: {last}")

    # -- consuming ----------------------------------------------------------
    def consume(self, spec: QueueSpec, handler: Handler, *, prefetch: int = 1) -> None:
        consumer = _RabbitConsumer(
            self._params, spec, handler, prefetch, self._max_redeliveries, self._retry_delay_ms
        )
        self._consumers.append(consumer)
        consumer.start()

    def close(self) -> None:
        for consumer in self._consumers:
            consumer.stop()
        for consumer in self._consumers:
            consumer.join(timeout=15)
        with self._lock:
            self._reset()


class _RabbitConsumer(threading.Thread):
    """Consumes one queue. Handlers run in a worker pool so the connection thread
    keeps servicing heartbeats during long jobs (e.g. FFmpeg transcodes)."""

    def __init__(self, params, spec: QueueSpec, handler: Handler, prefetch: int, max_redeliveries: int, retry_delay_ms: int):
        super().__init__(daemon=True, name=f"consumer-{spec.name}")
        self._params = params
        self._spec = spec
        self._handler = handler
        self._prefetch = max(1, prefetch)
        self._max_redeliveries = max_redeliveries
        self._retry_delay_ms = retry_delay_ms
        self._stop_event = threading.Event()
        self._inflight = 0
        self._inflight_lock = threading.Lock()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        backoff = 1.0
        while not self._stop_event.is_set():
            try:
                self._session()
                backoff = 1.0
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                log.warning("consumer %s lost connection: %s (retry in %.0fs)", self._spec.name, exc, backoff)
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, 15.0)

    def _session(self) -> None:
        conn = pika.BlockingConnection(self._params)
        executor = ThreadPoolExecutor(max_workers=self._prefetch, thread_name_prefix=f"h-{self._spec.name}")
        try:
            channel = conn.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=self._prefetch)

            def on_message(ch, method, props, body):
                with self._inflight_lock:
                    self._inflight += 1
                future = executor.submit(self._process_delivery, conn, ch, method, props, body)
                future.add_done_callback(self._log_unexpected_error)

            channel.basic_consume(queue=self._spec.name, on_message_callback=on_message)
            log.info("consuming %s", self._spec.name)
            while not self._stop_event.is_set():
                conn.process_data_events(time_limit=1)
            # graceful shutdown: let in-flight handlers finish and flush their acks
            deadline = time.monotonic() + 60
            while self._inflight > 0 and time.monotonic() < deadline:
                conn.process_data_events(time_limit=0.5)
        finally:
            executor.shutdown(wait=False)
            try:
                if conn.is_open:
                    conn.close()
            except Exception:  # noqa: BLE001
                pass

    def _log_unexpected_error(self, future) -> None:
        exc = future.exception()
        if exc is not None:  # never swallow errors raised inside the worker pool
            log.error("unexpected error in consumer %s: %r", self._spec.name, exc, exc_info=exc)

    def _process_delivery(self, conn, channel, method, props, body: bytes) -> None:
        tag = method.delivery_tag
        headers = dict(props.headers or {})
        routing_key = headers.get("x-original-routing-key") or method.routing_key
        redeliveries = int(headers.get("x-redelivery-count", 0))
        action = "ack"
        try:
            try:
                payload = json.loads(body)
                if not isinstance(payload, dict):
                    raise ValueError("message body must be a JSON object")
            except ValueError as exc:
                log.error("undecodable message on %s: %s", self._spec.name, exc)
                action = "reject"
            else:
                msg = Message(routing_key, payload, headers, props.message_id, redeliveries)
                try:
                    self._handler(msg)
                except RejectMessage as exc:
                    log.warning("message rejected on %s: %s", self._spec.name, exc)
                    action = "reject"
                except Exception:  # noqa: BLE001
                    log.exception("handler failed for %s (%s)", self._spec.name, routing_key)
                    action = "retry" if redeliveries < self._max_redeliveries else "reject"
        finally:
            self._finish(conn, channel, tag, action, body, props, headers, routing_key, redeliveries)

    def _finish(self, conn, channel, tag, action, body, props, headers, routing_key, redeliveries) -> None:
        def callback() -> None:
            if action == "ack":
                channel.basic_ack(tag)
            elif action == "reject":
                channel.basic_nack(tag, requeue=False)  # -> DLX -> <queue>.dlq
            else:
                new_headers = {**headers, "x-original-routing-key": routing_key, "x-redelivery-count": redeliveries + 1}
                channel.basic_publish(
                    exchange=EX_RETRY,
                    routing_key=self._spec.name,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=pika.DeliveryMode.Persistent,
                        message_id=props.message_id,
                        headers=new_headers,
                        expiration=str(self._retry_delay_ms),
                    ),
                )
                channel.basic_ack(tag)

        try:
            conn.add_callback_threadsafe(callback)
        except Exception:  # noqa: BLE001
            # Connection died while the handler ran: the broker will redeliver the
            # unacked message and handlers are idempotent.
            log.warning("could not settle message on %s (connection lost)", self._spec.name)
        finally:
            with self._inflight_lock:
                self._inflight -= 1


# --------------------------------------------------------------------------- #
# In-memory implementation (tests)
# --------------------------------------------------------------------------- #
def _topic_match(pattern: str, key: str) -> bool:
    regex = "^" + re.escape(pattern).replace(r"\*", r"[^.]+").replace(r"\#", r".*") + "$"
    return re.match(regex, key) is not None


class InMemoryBroker:
    """Synchronous broker. ``publish`` delivers inline (unless ``auto_pump`` is off)."""

    def __init__(self, *, max_redeliveries: int = 3, auto_pump: bool = True):
        self.max_redeliveries = max_redeliveries
        self.auto_pump = auto_pump
        self.fail_publish = False  # fault injection: simulate a broker outage
        self.published: list[tuple[str, str, dict[str, Any]]] = []
        self.dead_letters: dict[str, list[Message]] = defaultdict(list)
        self.delivery_log: list[tuple[str, str]] = []  # (queue, routing_key)
        self._queues: dict[str, deque[Message]] = {q.name: deque() for q in ALL_QUEUES}
        self._specs = {q.name: q for q in ALL_QUEUES}
        self._handlers: dict[str, Handler] = {}
        self._lock = threading.RLock()
        self._pumping = False

    # -- Broker API ---------------------------------------------------------
    def publish(self, exchange, routing_key, body, *, message_id=None, headers=None) -> None:
        if self.fail_publish:
            raise BrokerError("simulated broker outage")
        body = json.loads(json.dumps(body, default=str))
        with self._lock:
            self.published.append((exchange, routing_key, body))
            if exchange == EX_DLX:
                self.dead_letters[routing_key].append(Message(routing_key, body, dict(headers or {}), message_id))
            else:
                for spec in self._specs.values():
                    if spec.exchange == exchange and any(_topic_match(k, routing_key) for k in spec.routing_keys):
                        self._queues[spec.name].append(
                            Message(routing_key, body, dict(headers or {}), message_id or str(uuid.uuid4()))
                        )
        if self.auto_pump:
            self.pump()

    def consume(self, spec: QueueSpec, handler: Handler, *, prefetch: int = 1) -> None:
        with self._lock:
            self._handlers[spec.name] = handler
        if self.auto_pump:
            self.pump()

    def close(self) -> None:
        self._handlers.clear()

    # -- test helpers -------------------------------------------------------
    def pump(self) -> None:
        with self._lock:
            if self._pumping:
                return  # an outer pump() on this/another thread will drain the queue
            self._pumping = True
        try:
            while True:
                with self._lock:
                    item = self._next()
                if item is None:
                    break
                queue, msg = item
                handler = self._handlers[queue]
                self.delivery_log.append((queue, msg.routing_key))
                try:
                    handler(msg)
                except RejectMessage:
                    self.dead_letters[queue].append(msg)
                except Exception:  # noqa: BLE001
                    log.exception("in-memory handler error on %s", queue)
                    msg.redelivery_count += 1
                    if msg.redelivery_count > self.max_redeliveries:
                        self.dead_letters[queue].append(msg)
                    else:
                        with self._lock:
                            self._queues[queue].append(msg)
        finally:
            with self._lock:
                self._pumping = False

    def _next(self) -> tuple[str, Message] | None:
        for name, q in self._queues.items():
            if q and name in self._handlers:
                return name, q.popleft()
        return None

    def pending(self, queue: str) -> int:
        return len(self._queues[queue])

    def messages(self, routing_key: str) -> list[dict[str, Any]]:
        return [b for _, rk, b in self.published if rk == routing_key]

    def redeliver(self, queue: str, msg: Message) -> None:
        """Simulate the broker delivering the same message again (at-least-once)."""
        with self._lock:
            self._queues[queue].append(Message(msg.routing_key, dict(msg.body), dict(msg.headers), msg.message_id))
        self.pump()
