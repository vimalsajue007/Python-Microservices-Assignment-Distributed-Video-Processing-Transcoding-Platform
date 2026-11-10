"""Unit tests: broker semantics (in-memory implementation) and topology definitions."""

from __future__ import annotations

import pytest

from vp_common.locks import LockBusy, LockManager, LockUnavailable
from vp_common.messaging import BrokerError, InMemoryBroker, Message, RejectMessage
from vp_common.security import sign, verify
from vp_common.topology import (
    ALL_QUEUES, EX_DLX, EX_EVENTS, EX_JOBS, Q_METADATA, Q_ORCHESTRATOR, Q_THUMBNAIL, Q_TRANSCODE,
)

from helpers.fakes import FakeRedis


def test_events_fan_out_to_every_bound_queue():
    broker = InMemoryBroker()
    seen = {"metadata": [], "thumbnail": []}
    broker.consume(Q_METADATA, lambda m: seen["metadata"].append(m.routing_key))
    broker.consume(Q_THUMBNAIL, lambda m: seen["thumbnail"].append(m.routing_key))
    broker.publish(EX_EVENTS, "video.uploaded", {"video_id": "1"})
    broker.publish(EX_EVENTS, "job.status_changed", {"x": 1})
    assert seen == {"metadata": ["video.uploaded", "job.status_changed"], "thumbnail": ["video.uploaded"]}


def test_work_queue_delivers_to_a_single_consumer_queue():
    broker = InMemoryBroker()
    got = []
    broker.consume(Q_TRANSCODE, lambda m: got.append(m.body))
    broker.consume(Q_METADATA, lambda m: got.append("wrong"))
    broker.publish(EX_JOBS, "transcode", {"job": 1})
    assert got == [{"job": 1}]


def test_messages_wait_in_the_queue_until_a_consumer_starts():
    broker = InMemoryBroker()
    broker.publish(EX_EVENTS, "video.uploaded", {"id": 1})  # nobody consuming yet: must not be lost
    assert broker.pending("metadata.events") == 1 and broker.pending("thumbnail.events") == 1
    got = []
    broker.consume(Q_METADATA, lambda m: got.append(m.body["id"]))
    assert got == [1] and broker.pending("metadata.events") == 0


def test_handler_exception_triggers_redelivery_then_dead_letter():
    broker = InMemoryBroker(max_redeliveries=2)
    attempts = []

    def failing(msg: Message):
        attempts.append(msg.redelivery_count)
        raise RuntimeError("db down")

    broker.consume(Q_METADATA, failing)
    broker.publish(EX_EVENTS, "video.uploaded", {"id": 1})
    assert attempts == [0, 1, 2]  # initial delivery + 2 redeliveries
    assert len(broker.dead_letters["metadata.events"]) == 1


def test_transient_failure_is_retried_until_it_succeeds():
    broker = InMemoryBroker()
    state = {"calls": 0}

    def flaky(msg):
        state["calls"] += 1
        if state["calls"] < 3:
            raise RuntimeError("temporary")

    broker.consume(Q_METADATA, flaky)
    broker.publish(EX_EVENTS, "video.uploaded", {})
    assert state["calls"] == 3 and not broker.dead_letters


def test_reject_dead_letters_immediately_without_retry():
    broker = InMemoryBroker()
    calls = []

    def poison(msg):
        calls.append(1)
        raise RejectMessage("bad payload")

    broker.consume(Q_TRANSCODE, poison)
    broker.publish(EX_JOBS, "transcode", {"bad": True})
    assert calls == [1] and len(broker.dead_letters["transcode.jobs"]) == 1


def test_publishing_to_the_dead_letter_exchange_lands_in_the_dlq():
    broker = InMemoryBroker()
    broker.publish(EX_DLX, "transcode.jobs", {"job_id": "x"})
    assert broker.dead_letters["transcode.jobs"][0].body == {"job_id": "x"}


def test_publish_outage_raises_broker_error():
    broker = InMemoryBroker()
    broker.fail_publish = True
    with pytest.raises(BrokerError):
        broker.publish(EX_EVENTS, "video.uploaded", {})


def test_handlers_may_publish_without_recursing():
    broker = InMemoryBroker()
    order = []

    def metadata(msg):
        order.append("metadata")
        broker.publish(EX_EVENTS, "job.progress", {})  # nested publish is queued, not re-entered

    broker.consume(Q_METADATA, metadata)
    broker.consume(Q_ORCHESTRATOR, lambda m: order.append("orchestrator"))
    broker.publish(EX_EVENTS, "video.uploaded", {})
    assert order == ["metadata", "orchestrator"]


def test_topology_every_queue_has_dead_letter_and_retry_companions():
    names = {q.name for q in ALL_QUEUES}
    assert names == {"metadata.events", "orchestrator.events", "thumbnail.events", "transcode.jobs"}
    for q in ALL_QUEUES:
        assert q.dlq == f"{q.name}.dlq" and q.retry_queue == f"{q.name}.retry"


def test_topology_declaration_is_complete_and_idempotent():
    class Recorder:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            return lambda **kw: self.calls.append((name, kw))

    ch = Recorder()
    from vp_common.topology import declare_topology

    declare_topology(ch)
    declared = [kw["queue"] for n, kw in ch.calls if n == "queue_declare"]
    assert set(declared) == {x for q in ALL_QUEUES for x in (q.name, q.dlq, q.retry_queue)}
    main = next(kw for n, kw in ch.calls if n == "queue_declare" and kw["queue"] == "transcode.jobs")
    assert main["arguments"]["x-dead-letter-exchange"] == EX_DLX
    retry = next(kw for n, kw in ch.calls if n == "queue_declare" and kw["queue"] == "transcode.jobs.retry")
    assert retry["arguments"]["x-dead-letter-routing-key"] == "transcode.jobs"


# --------------------------------------------------------------------------- #
def test_lock_manager_semantics():
    redis = FakeRedis()
    locks = LockManager(redis)
    assert locks.acquire("k", "a", 30) is True
    assert locks.acquire("k", "b", 30) is False  # held by someone else
    assert locks.acquire("k", "a", 30) is True  # re-entrant for the same owner
    locks.release("k", "b")  # a non-owner cannot release
    assert locks.acquire("k", "b", 30) is False
    locks.release("k", "a")
    assert locks.acquire("k", "b", 30) is True


def test_lock_hold_context_manager_and_outage():
    redis = FakeRedis()
    locks = LockManager(redis)
    with locks.hold("job", "me", 10):
        with pytest.raises(LockBusy):
            with locks.hold("job", "other", 10):
                pass
    with locks.hold("job", "other", 10):  # released on exit
        pass
    redis.down = True
    with locks.hold("job", "me", 10):  # fail-open by default
        pass
    with pytest.raises(LockUnavailable):
        with locks.hold("job", "me", 10, fail_open=False):
            pass


def test_signed_urls_verify_and_expire():
    sig = sign("secret", "/media/outputs/x/360p.mp4", 2_000_000_000)
    assert verify("secret", "/media/outputs/x/360p.mp4", 2_000_000_000, sig, now=1_999_999_000)
    assert not verify("secret", "/media/outputs/x/360p.mp4", 2_000_000_000, sig, now=2_000_000_001)  # expired
    assert not verify("other", "/media/outputs/x/360p.mp4", 2_000_000_000, sig, now=1_999_999_000)  # wrong key
    assert not verify("secret", "/media/outputs/y/360p.mp4", 2_000_000_000, sig, now=1_999_999_000)  # other path
    assert not verify("secret", "/media/outputs/x/360p.mp4", 2_000_000_000, "", now=1_999_999_000)
