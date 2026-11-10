"""RabbitMQ topology: exchanges, queues, routing keys.

Every queue gets two companions:

* ``<queue>.retry`` - delay queue (per-message TTL) that dead-letters back to
  ``<queue>`` through the default exchange. Used for consumer-level redelivery
  after transient infrastructure errors.
* ``<queue>.dlq``   - dead-letter queue. Poison messages, messages that exhausted
  their redeliveries and jobs that exhausted all processing attempts end up here.

Topology is declared idempotently by *every* publisher and consumer on connect,
so an event published before its consumer has started is never dropped.
"""

from __future__ import annotations

from dataclasses import dataclass

EX_EVENTS = "vp.events"  # topic    - domain events (fan-out)
EX_JOBS = "vp.jobs"  # direct   - work queue messages
EX_DLX = "vp.dlx"  # direct   - dead-letter exchange
EX_RETRY = "vp.retry"  # direct   - delayed redelivery

# Routing keys ---------------------------------------------------------------
RK_VIDEO_UPLOADED = "video.uploaded"
RK_JOB_STATUS = "job.status_changed"
RK_JOB_STARTED = "job.attempt.started"
RK_JOB_PROGRESS = "job.progress"
RK_JOB_SUCCEEDED = "job.attempt.succeeded"
RK_JOB_FAILED = "job.attempt.failed"
RK_THUMBNAIL = "thumbnail.generated"
RK_TRANSCODE = "transcode"


@dataclass(frozen=True)
class QueueSpec:
    name: str
    exchange: str
    routing_keys: tuple[str, ...]

    @property
    def retry_queue(self) -> str:
        return f"{self.name}.retry"

    @property
    def dlq(self) -> str:
        return f"{self.name}.dlq"


Q_METADATA = QueueSpec("metadata.events", EX_EVENTS, (RK_VIDEO_UPLOADED, RK_JOB_STATUS))
Q_ORCHESTRATOR = QueueSpec(
    "orchestrator.events",
    EX_EVENTS,
    (RK_JOB_STARTED, RK_JOB_PROGRESS, RK_JOB_SUCCEEDED, RK_JOB_FAILED),
)
Q_THUMBNAIL = QueueSpec("thumbnail.events", EX_EVENTS, (RK_VIDEO_UPLOADED,))
Q_TRANSCODE = QueueSpec("transcode.jobs", EX_JOBS, (RK_TRANSCODE,))

ALL_QUEUES: tuple[QueueSpec, ...] = (Q_METADATA, Q_ORCHESTRATOR, Q_THUMBNAIL, Q_TRANSCODE)
EXCHANGES: tuple[tuple[str, str], ...] = (
    (EX_EVENTS, "topic"),
    (EX_JOBS, "direct"),
    (EX_DLX, "direct"),
    (EX_RETRY, "direct"),
)


def declare_topology(channel) -> None:
    """Declare exchanges, queues, bindings. Safe to call repeatedly."""
    for name, kind in EXCHANGES:
        channel.exchange_declare(exchange=name, exchange_type=kind, durable=True)
    for q in ALL_QUEUES:
        channel.queue_declare(
            queue=q.name,
            durable=True,
            arguments={"x-dead-letter-exchange": EX_DLX, "x-dead-letter-routing-key": q.name},
        )
        channel.queue_declare(queue=q.dlq, durable=True)
        channel.queue_bind(queue=q.dlq, exchange=EX_DLX, routing_key=q.name)
        channel.queue_declare(
            queue=q.retry_queue,
            durable=True,
            arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": q.name},
        )
        channel.queue_bind(queue=q.retry_queue, exchange=EX_RETRY, routing_key=q.name)
        for key in q.routing_keys:
            channel.queue_bind(queue=q.name, exchange=q.exchange, routing_key=key)
