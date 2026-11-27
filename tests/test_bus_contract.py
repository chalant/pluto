"""The bus contract (pluto/bus/__init__.py, rules S1 to S14), run on every implementation.

`BusContract` holds the tests; one subclass per implementation supplies `make_bus`. Payloads are JSON-compatible
(or protobuf Timestamps) so that the same assertions hold with serialisation on and off.

Run as `python -m tests.test_bus_contract <implementation>` it prints the delivery trace of a fixed scenario,
which the hash-seed test compares across interpreters started with different `PYTHONHASHSEED` values.
"""
import json
import os
import random
import subprocess
import sys
import unittest
from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

from pluto.bus import (
    BusClosed,
    BusError,
    Delivery,
    Envelope,
    InMemoryBus,
    NotIdle,
    ProtobufCodec,
    SerializingBus,
    Start,
    reply,
    request,
)
from pluto.bus.log import LogBus
from tests import nats_support

REPO = Path(__file__).resolve().parent.parent
HASH_SEEDS = ("1", "2", "3", "4", "5")


def make_memory(**options):
    return InMemoryBus(**options)


def make_serializing(**options):
    return SerializingBus(InMemoryBus(**options), ProtobufCodec({"timestamp": Timestamp}))


def make_nats(**options):
    """The log-backed bus over a new JetStream stream (imported here: a venv without nats-py skips the NATS class)."""
    return SerializingBus(
        LogBus(nats_support.jetstream.JetStreamLog(drop_on_close=True), **options),
        ProtobufCodec({"timestamp": Timestamp}),
    )


FACTORIES = {"memory": make_memory, "serializing": make_serializing, "nats": make_nats}


def publish(bus, topic, payload=None, *, key="", ts=0, producer="p", **fields):
    return bus.publish(topic, payload, type="t", logical_ts=ts, producer_id=producer, key=key, **fields)


class Recorder:
    """Handlers that append (name, msg_id, attempt) to one shared list."""

    def __init__(self):
        self.calls = []

    def handler(self, name, action=None):
        def handle(delivery):
            self.calls.append((name, delivery.envelope.msg_id, delivery.attempt))
            if action is not None:
                action(delivery)

        return handle

    def names(self):
        return [call[0] for call in self.calls]

    def ids(self, name=None):
        return [call[1] for call in self.calls if name is None or call[0] == name]


def scenario_trace(make_bus):
    """A fixed workload that exercises every ordering path: ties, phases, wildcards, ranks, a group, failures with
    dead letters, cascades, checkpoints."""
    bus = make_bus(max_attempts=3)
    rec = Recorder()
    checkpoints = []
    rng = random.Random(1234)

    def flaky(delivery):
        if delivery.envelope.seq % 7 == 0 and delivery.attempt == 1:
            raise RuntimeError("transient")

    def poison(delivery):
        if delivery.envelope.seq % 11 == 0:
            raise ValueError("poison %d" % delivery.envelope.seq)

    def follow_up(delivery):
        env = delivery.envelope
        if env.seq % 5 == 0 and env.topic.startswith("orders."):
            publish(bus, "fills." + env.topic[7:], env.payload, key=env.key, ts=env.logical_ts, producer="fill-" + env.key)

    def checkpoint(delivery):
        checkpoints.append((delivery.envelope.msg_id, list(bus.position_of(delivery))))

    bus.subscribe("orders.*", rec.handler("orders", follow_up), rank=1)
    bus.subscribe("orders.a", rec.handler("orders-a", flaky), rank=0)
    bus.subscribe("orders.b", rec.handler("poison", poison), rank=2)
    bus.subscribe("fills.>", rec.handler("fills"))
    bus.subscribe(">", rec.handler("all"), rank=5, start=Start.LATEST)
    bus.subscribe("orders.*", rec.handler("g1"), group="g")
    bus.subscribe("orders.*", rec.handler("g2"), group="g")
    bus.subscribe("orders.*", checkpoint, rank=9)
    for i in range(200):
        topic = ("orders.a", "orders.b", "orders.c")[rng.randrange(3)]
        publish(
            bus,
            topic,
            i,
            key="k%d" % rng.randrange(6),
            ts=rng.randrange(10),
            phase=rng.randrange(3),
            producer="prod-%d" % rng.randrange(4),
        )
        if i % 40 == 39:
            bus.run_until_idle()
    bus.run_until_idle()
    late = Recorder()
    bus.subscribe("fills.>", late.handler("late"), start=Start.EARLIEST)
    bus.run_until_idle()
    trace = {
        "calls": rec.calls,
        "late": late.calls,
        "checkpoints": checkpoints,
        "dead": [(d.envelope.msg_id, d.attempts, d.error_type, d.error_message) for d in bus.dead_letters()],
    }
    bus.close()  # a networked bus leaves its stream otherwise
    return trace


class BusContract:
    make_bus = None  # (**options) -> bus; set by the subclasses

    def setUp(self):
        self.rec = Recorder()

    def bus(self, **options):
        bus = type(self).make_bus(**options)
        self.addCleanup(bus.close)
        return bus

    # S1 ordering

    def test_fifo_per_topic_and_key(self):
        bus = self.bus()
        seen = []
        bus.subscribe("t.>", lambda d: seen.append((d.envelope.topic, d.envelope.key, d.envelope.payload)))
        rng = random.Random(5)
        sent = []
        for i in range(60):
            topic, key = "t.%d" % rng.randrange(2), "k%d" % rng.randrange(3)
            publish(bus, topic, i, key=key, ts=rng.randrange(1000), producer="p%d" % rng.randrange(3))
            sent.append((topic, key, i))
        bus.run_until_idle()
        self.assertEqual(len(seen), 60)
        for lane in {(t, k) for t, k, _ in sent}:
            self.assertEqual([p for t, k, p in seen if (t, k) == lane], [p for t, k, p in sent if (t, k) == lane])

    def test_each_subscription_sees_the_whole_stream_once(self):
        bus = self.bus()
        first, second = [], []
        bus.subscribe("t", lambda d: first.append(d.envelope.payload))
        bus.subscribe("t", lambda d: second.append(d.envelope.payload))
        for i in range(5):
            publish(bus, "t", i)
        bus.run_until_idle()
        self.assertEqual(first, [0, 1, 2, 3, 4])
        self.assertEqual(second, [0, 1, 2, 3, 4])

    # S3 deterministic dispatch

    def test_dispatch_picks_lowest_logical_ts_then_producer_then_seq(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", key="k1", ts=5, producer="b", seq=1, msg_id="b5")
        publish(bus, "t", key="k2", ts=5, producer="a", seq=2, msg_id="a5-2")
        publish(bus, "t", key="k3", ts=1, producer="z", seq=9, msg_id="z1")
        publish(bus, "t", key="k4", ts=5, producer="a", seq=1, msg_id="a5-1")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["z1", "a5-1", "a5-2", "b5"])

    def test_dispatch_does_not_depend_on_publish_order_across_lanes(self):
        def run(order):
            bus = self.bus()
            rec = Recorder()
            bus.subscribe("t", rec.handler("s"))
            for ts in order:
                publish(bus, "t", key="k%d" % ts, ts=ts, msg_id="m%d" % ts)
            bus.run_until_idle()
            return rec.ids()

        self.assertEqual(run([3, 1, 2]), ["m1", "m2", "m3"])
        self.assertEqual(run([1, 2, 3]), ["m1", "m2", "m3"])

    def test_equal_content_falls_back_to_registration_order(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("first"))
        bus.subscribe("t", self.rec.handler("second"))
        publish(bus, "t", ts=1, seq=1, msg_id="m")
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["first", "second"])

    def test_equal_content_falls_back_to_topic_then_key(self):
        bus = self.bus()
        bus.subscribe("t.*", self.rec.handler("s"))
        for topic, key in (("t.b", "k1"), ("t.a", "k2"), ("t.a", "k1"), ("t.b", "k0")):
            publish(bus, topic, key=key, ts=1, seq=1, msg_id=topic + key)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["t.ak1", "t.ak2", "t.bk0", "t.bk1"])

    def test_a_replay_delivers_in_the_order_a_live_subscriber_saw(self):
        bus = self.bus()
        live, replayed = Recorder(), Recorder()
        bus.subscribe("t.*", live.handler("s"))
        for topic, key in (("t.b", "k1"), ("t.a", "k2"), ("t.b", "k0"), ("t.a", "k1"), ("t.c", "k0"), ("t.a", "k0")):
            publish(bus, topic, key=key, ts=1, seq=1, msg_id=topic + key)
        bus.run_until_idle()
        bus.subscribe("t.*", replayed.handler("s"), start=Start.EARLIEST)
        bus.run_until_idle()
        self.assertEqual(len(live.ids()), 6)
        self.assertEqual(replayed.ids(), live.ids())

    def test_phase_orders_messages_of_the_same_logical_ts(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", key="a", ts=5, phase=2, producer="a", msg_id="phase2")
        publish(bus, "t", key="b", ts=5, phase=0, producer="z", msg_id="phase0")
        publish(bus, "t", key="c", ts=5, phase=1, producer="m", msg_id="phase1")
        publish(bus, "t", key="d", ts=4, phase=9, producer="q", msg_id="earlier")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["earlier", "phase0", "phase1", "phase2"])

    def test_dispatch_is_across_topics_too(self):
        bus = self.bus()
        bus.subscribe("a.*", self.rec.handler("s"))
        publish(bus, "a.x", ts=2, msg_id="late")
        publish(bus, "a.y", ts=1, msg_id="early")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["early", "late"])

    def test_delivery_trace_is_repeatable(self):
        first = scenario_trace(type(self).make_bus)
        second = scenario_trace(type(self).make_bus)
        self.assertEqual(first, second)
        self.assertGreater(len(first["calls"]), 400)
        self.assertTrue(any(attempt > 1 for _, _, attempt in first["calls"]))
        self.assertTrue(first["late"])

    def test_delivery_trace_is_independent_of_hash_seed(self):
        expected = json.loads(json.dumps(scenario_trace(type(self).make_bus)))
        for seed in HASH_SEEDS:
            env = dict(os.environ, PYTHONHASHSEED=seed)
            out = subprocess.run(
                [sys.executable, "-m", "tests.test_bus_contract", self.implementation],
                cwd=REPO,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            self.assertEqual(json.loads(out), expected, "PYTHONHASHSEED=" + seed)

    # S2 no nested delivery

    def test_publish_never_runs_a_handler(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t")
        self.assertEqual(self.rec.calls, [])
        self.assertEqual(bus.run_until_idle(), 1)

    def test_a_handler_that_publishes_is_not_interrupted(self):
        bus = self.bus()
        events = []

        def on_a(delivery):
            events.append("a start")
            publish(bus, "b")
            events.append("a published")

        bus.subscribe("a", on_a)
        bus.subscribe("b", lambda d: events.append("b"))
        publish(bus, "a")
        bus.run_until_idle()
        self.assertEqual(events, ["a start", "a published", "b"])

    def test_step_and_run_until_idle_refuse_to_nest(self):
        bus = self.bus()
        errors = []

        def handler(delivery):
            for call in (bus.step, bus.run_until_idle):
                try:
                    call()
                except BusError as error:
                    errors.append(error)

        bus.subscribe("t", handler)
        publish(bus, "t")
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(len(errors), 4)

    # S6 rank

    def test_handler_rank_orders_the_handlers_of_a_message(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("rank2"), rank=2)
        bus.subscribe("t", self.rec.handler("rank0-first"), rank=0)
        bus.subscribe("t", self.rec.handler("rank1"), rank=1)
        bus.subscribe("t", self.rec.handler("rank0-second"), rank=0)
        publish(bus, "t", ts=1, msg_id="m1")
        publish(bus, "t", ts=2, msg_id="m2")
        bus.run_until_idle()
        order = ["rank0-first", "rank0-second", "rank1", "rank2"]
        self.assertEqual(self.rec.calls, [(name, m, 1) for m in ("m1", "m2") for name in order])

    def test_default_rank_is_registration_order(self):
        bus = self.bus()
        for name in ("a", "b", "c"):
            bus.subscribe("t", self.rec.handler(name))
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["a", "b", "c"])

    # S7 failures

    def test_exception_is_a_nack_and_a_redelivery(self):
        bus = self.bus()

        def flaky(delivery):
            if delivery.attempt == 1:
                raise RuntimeError("transient")

        bus.subscribe("t", self.rec.handler("flaky", flaky))
        bus.subscribe("t", self.rec.handler("steady"), rank=1)
        publish(bus, "t", {"n": 1}, msg_id="m")
        bus.run_until_idle()
        # the retry keeps its dispatch key, so it runs again before the higher-ranked handler of the message
        self.assertEqual(
            self.rec.calls, [("flaky", "m", 1), ("flaky", "m", 2), ("steady", "m", 1)]
        )
        self.assertEqual(bus.dead_letters(), ())

    def test_nack_redelivers_before_the_next_message_of_the_lane(self):
        bus = self.bus()
        seen = []

        def handler(delivery):
            seen.append((delivery.envelope.payload, delivery.attempt))
            if delivery.envelope.payload == 1 and delivery.attempt < 3:
                bus.nack(delivery)

        bus.subscribe("t", handler)
        for i in range(3):
            publish(bus, "t", i, key="k")
        bus.run_until_idle()
        self.assertEqual(seen, [(0, 1), (1, 1), (1, 2), (1, 3), (2, 1)])
        self.assertEqual(bus.dead_letters(), ())

    def test_ack_after_nack_wins_and_an_exception_always_nacks(self):
        bus = self.bus()

        def handler(delivery):
            bus.nack(delivery)
            bus.ack(delivery)
            if delivery.envelope.payload == "raise":
                raise RuntimeError("late failure")

        bus.subscribe("t", self.rec.handler("s", handler))
        publish(bus, "t", "ok", msg_id="ok")
        publish(bus, "t", "raise", msg_id="raise", ts=1)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids().count("ok"), 1)
        self.assertEqual(self.rec.ids().count("raise"), 3)

    def test_failures_never_reach_the_publisher_or_the_driver(self):
        bus = self.bus()

        def boom(delivery):
            raise RuntimeError("always")

        bus.subscribe("t", boom)
        receipt = publish(bus, "t")
        self.assertEqual(bus.run_until_idle(), 3)
        self.assertEqual(receipt.msg_id, "p:1")

    def test_an_exit_request_propagates_and_leaves_the_message_pending(self):
        class Abort(BaseException):
            pass

        bus = self.bus()
        seen = []

        def handler(delivery):
            seen.append(delivery.attempt)
            if len(seen) == 1:
                raise Abort

        bus.subscribe("t", handler)
        publish(bus, "t")
        with self.assertRaises(Abort):
            bus.step()
        self.assertEqual(bus.run_until_idle(), 1)
        self.assertEqual(seen, [1, 1])
        self.assertEqual(bus.dead_letters(), ())

    def test_dead_letter_after_max_attempts_and_the_lane_moves_on(self):
        bus = self.bus(max_attempts=3)
        error = RuntimeError("always")

        def handler(delivery):
            if delivery.envelope.payload == "poison":
                raise error

        bus.subscribe("t", self.rec.handler("s", handler))
        publish(bus, "t", "poison", key="k", msg_id="bad")
        publish(bus, "t", "fine", key="k", msg_id="good")
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("s", "bad", 1), ("s", "bad", 2), ("s", "bad", 3), ("s", "good", 1)])
        (letter,) = bus.dead_letters()
        self.assertEqual((letter.envelope.msg_id, letter.envelope.payload), ("bad", "poison"))
        self.assertEqual(letter.attempts, 3)
        self.assertEqual((letter.error_type, letter.error_message), ("RuntimeError", "always"))

    def test_explicit_nack_dead_letter_has_no_error(self):
        bus = self.bus(max_attempts=2)
        bus.subscribe("t", lambda d: bus.nack(d))
        publish(bus, "t")
        bus.run_until_idle()
        (letter,) = bus.dead_letters()
        self.assertIsNone(letter.error_type)
        self.assertIsNone(letter.error_message)
        self.assertEqual(letter.attempts, 2)

    def test_a_dead_letter_is_per_subscription(self):
        bus = self.bus(max_attempts=2)

        def boom(delivery):
            raise RuntimeError("always")

        failing = bus.subscribe("t", boom)
        bus.subscribe("t", self.rec.handler("ok"), rank=1)
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids("ok"), ["p:1"])
        self.assertEqual([letter.subscription for letter in bus.dead_letters()], [failing.id])

    def test_the_bus_does_not_deduplicate(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", msg_id="same")
        publish(bus, "t", msg_id="same")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["same", "same"])

    def test_ack_and_nack_apply_only_to_the_running_delivery(self):
        bus = self.bus()
        held = []
        bus.subscribe("t", held.append)
        publish(bus, "t")
        with self.assertRaises(BusError):
            bus.ack(_fake_delivery())
        bus.run_until_idle()
        with self.assertRaises(BusError):
            bus.nack(held[0])
        with self.assertRaises(BusError):
            bus.ack(held[0])

    # S9 replay

    def _filled(self, n=5):
        bus = self.bus()
        receipts = [publish(bus, "t", i, ts=i, msg_id="m%d" % i) for i in range(n)]
        return bus, receipts

    def test_replay_from_earliest(self):
        bus, _ = self._filled()
        bus.subscribe("t", self.rec.handler("s"), start=Start.EARLIEST)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m0", "m1", "m2", "m3", "m4"])

    def test_latest_skips_the_log_and_gets_new_messages(self):
        bus, _ = self._filled()
        bus.subscribe("t", self.rec.handler("default"))
        bus.subscribe("t", self.rec.handler("latest"), start=Start.LATEST)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])
        publish(bus, "t", msg_id="new", ts=10)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("default", "new", 1), ("latest", "new", 1)])

    def test_replay_from_a_receipt_position_is_inclusive(self):
        bus, receipts = self._filled()
        bus.subscribe("t", self.rec.handler("s"), start=receipts[2].position)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m2", "m3", "m4"])

    def test_resume_from_the_position_of_a_delivery_continues_after_it(self):
        bus = self.bus()
        checkpoint = []

        def consume(delivery):
            if delivery.envelope.msg_id == "m2":
                checkpoint.append(bus.position_of(delivery))

        bus.subscribe("t", consume)
        for i in range(5):
            publish(bus, "t", i, ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        bus.subscribe("t", self.rec.handler("resumed"), start=checkpoint[0])
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m3", "m4"])

    def test_a_position_at_the_end_of_the_log_waits_for_the_next_message(self):
        bus = self.bus()
        ends = []
        bus.subscribe("t", lambda d: ends.append(bus.position_of(d)))
        for i in range(3):
            publish(bus, "t", i, ts=i)
        bus.run_until_idle()
        bus.subscribe("t", self.rec.handler("s"), start=ends[-1])
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])
        publish(bus, "t", msg_id="next", ts=9)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["next"])

    def test_replay_leaves_existing_subscriptions_alone(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("old"))
        publish(bus, "t", msg_id="m0")
        bus.run_until_idle()
        bus.subscribe("t", self.rec.handler("new"), start=Start.EARLIEST)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("old", "m0", 1), ("new", "m0", 1)])

    def test_replay_then_live_messages_follow_the_dispatch_rule(self):
        bus, _ = self._filled(3)
        bus.subscribe("t", self.rec.handler("s"), start=Start.EARLIEST)
        publish(bus, "t", key="other", msg_id="live", ts=-1)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["live", "m0", "m1", "m2"])

    def test_replay_over_a_pattern_merges_topics_by_the_dispatch_rule(self):
        bus = self.bus()
        publish(bus, "a.x", ts=3, msg_id="a3")
        publish(bus, "b.x", ts=1, msg_id="b1")
        publish(bus, "a.y", ts=2, msg_id="a2")
        bus.subscribe("a.*", self.rec.handler("s"), start=Start.EARLIEST)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["a2", "a3"])

    def test_position_of_is_a_resume_point_without_gaps_when_keys_complete_out_of_log_order(self):
        bus = self.bus()
        publish(bus, "t", key="k1", ts=100, msg_id="late")  # log offset 0, dispatched second
        publish(bus, "t", key="k2", ts=10, msg_id="early")  # log offset 1, dispatched first
        first = []
        bus.subscribe("t", first.append, start=Start.EARLIEST)
        self.assertTrue(bus.step())
        self.assertEqual(first[0].envelope.msg_id, "early")
        checkpoint = bus.position_of(first[0])
        bus.subscribe("t", self.rec.handler("resumed"), start=checkpoint)
        bus.run_until_idle()
        # "late" was never delivered to the first subscription, so the checkpoint is before it; "early" comes
        # again (at-least-once)
        self.assertEqual(self.rec.ids(), ["early", "late"])

    def test_position_of_after_everything_is_settled_resumes_with_nothing_old(self):
        bus = self.bus()
        positions = []
        bus.subscribe("t", lambda d: positions.append(bus.position_of(d)))
        for i in range(4):
            publish(bus, "t", i, key="k%d" % (i % 2), ts=10 - i, msg_id="m%d" % i)
        bus.run_until_idle()
        self.assertEqual({p.topic for p in positions}, {"t"})
        bus.subscribe("t", self.rec.handler("resumed"), start=positions[-1])
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])
        publish(bus, "t", msg_id="new", ts=99)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["new"])

    def test_a_dead_lettered_message_counts_as_settled_for_the_position(self):
        bus = self.bus(max_attempts=2)
        positions = []

        def handler(delivery):
            if delivery.envelope.msg_id == "bad":
                raise RuntimeError("always")
            positions.append(bus.position_of(delivery))

        bus.subscribe("t", handler)
        publish(bus, "t", msg_id="bad", ts=1)
        publish(bus, "t", msg_id="good", ts=2)
        bus.run_until_idle()
        bus.subscribe("t", self.rec.handler("resumed"), start=positions[-1])
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])

    def test_subscribing_never_delivers_inline(self):
        bus, receipts = self._filled(3)
        bus.subscribe("t", self.rec.handler("from earliest"), start=Start.EARLIEST)
        bus.subscribe("t", self.rec.handler("from position"), start=receipts[1].position)
        self.assertEqual(self.rec.calls, [])
        self.assertEqual(bus.run_until_idle(), 5)

    def test_replay_redelivers_with_attempt_one_and_the_original_envelope(self):
        bus = self.bus()
        receipt = publish(bus, "t", {"a": [1, 2]}, key="k", ts=7, producer="q", correlation_id="c", reply_to="r")
        seen = []
        bus.subscribe("t", seen.append, start=receipt.position)
        bus.run_until_idle()
        env = seen[0].envelope
        self.assertEqual(seen[0].attempt, 1)
        self.assertEqual(
            (env.msg_id, env.topic, env.key, env.logical_ts, env.producer_id, env.seq, env.correlation_id, env.reply_to),
            ("q:1", "t", "k", 7, "q", 1, "c", "r"),
        )
        self.assertEqual(env.payload, {"a": [1, 2]})

    # S10 request and reply

    def _responder(self, bus, drop_first_reply=False):
        """A receiver idempotent on msg_id: it does the work once and answers every delivery from its record."""
        work, results, seen = [], {}, []

        def respond(delivery):
            env = delivery.envelope
            seen.append(env.msg_id)
            if env.msg_id not in results:
                work.append(env.msg_id)
                results[env.msg_id] = {"echo": env.payload}
            if drop_first_reply and len(seen) == 1:
                return  # the reply is lost on its way back
            reply(bus, delivery, results[env.msg_id], type="t", logical_ts=env.logical_ts, producer_id="svc")

        bus.subscribe("svc.orders", respond)
        return work, seen

    def test_request_gets_its_reply(self):
        bus = self.bus()
        work, seen = self._responder(bus)
        pending = request(bus, "svc.orders", {"qty": 3}, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        self.assertFalse(pending.done)
        bus.run_until_idle()
        self.assertTrue(pending.done)
        self.assertEqual(pending.reply.payload, {"echo": {"qty": 3}})
        self.assertEqual(pending.reply.correlation_id, "strat:1")
        self.assertEqual(pending.reply.producer_id, "svc")
        self.assertEqual(work, ["strat:1"])

    def test_a_lost_reply_is_recovered_by_a_retry_with_the_same_msg_id(self):
        bus = self.bus()
        work, seen = self._responder(bus, drop_first_reply=True)
        pending = request(bus, "svc.orders", {"qty": 3}, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        bus.run_until_idle()
        self.assertFalse(pending.done)
        pending.send(logical_ts=2)
        bus.run_until_idle()
        self.assertTrue(pending.done)
        self.assertEqual(pending.reply.payload, {"echo": {"qty": 3}})
        self.assertEqual(seen, ["strat:1", "strat:1"])  # the bus redelivered, it did not deduplicate
        self.assertEqual(work, ["strat:1"])  # the receiver did the work once

    def test_a_late_duplicate_reply_is_ignored(self):
        bus = self.bus()
        self._responder(bus)
        pending = request(bus, "svc.orders", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        pending.send()
        bus.run_until_idle()
        self.assertTrue(pending.done)
        first = pending.reply
        pending.send()
        bus.run_until_idle()
        self.assertIs(pending.reply, first)

    def test_replies_and_requests_are_logged_and_replayable(self):
        bus = self.bus()
        self._responder(bus)
        pending = request(bus, "svc.orders", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        bus.run_until_idle()
        bus.subscribe(pending.reply_to, self.rec.handler("audit"), start=Start.EARLIEST)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["strat:1:reply"])

    def test_requests_do_not_see_each_others_replies(self):
        bus = self.bus()
        self._responder(bus)
        one = request(bus, "svc.orders", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        two = request(bus, "svc.orders", 2, type="t", logical_ts=1, producer_id="strat", msg_id="strat:2")
        bus.run_until_idle()
        self.assertEqual((one.reply.payload, two.reply.payload), ({"echo": 1}, {"echo": 2}))

    def test_a_cancelled_request_ignores_its_reply(self):
        bus = self.bus()
        self._responder(bus)
        pending = request(bus, "svc.orders", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        pending.cancel()
        bus.run_until_idle()
        self.assertFalse(pending.done)

    def test_reply_needs_a_reply_to(self):
        bus = self.bus()
        errors = []

        def handler(delivery):
            try:
                reply(bus, delivery, None, type="t", logical_ts=0, producer_id="svc")
            except ValueError as error:
                errors.append(error)

        bus.subscribe("t", handler)
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(len(errors), 1)

    def test_requests_of_one_producer_share_its_reply_inbox(self):
        bus = self.bus()
        self._responder(bus)
        one = request(bus, "svc.orders", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        two = request(bus, "svc.orders", 2, type="t", logical_ts=1, producer_id="strat", msg_id="strat:2")
        other = request(bus, "svc.orders", 3, type="t", logical_ts=1, producer_id="other", msg_id="other:1")
        self.assertEqual((one.reply_to, two.reply_to, other.reply_to), ("reply.strat", "reply.strat", "reply.other"))
        bus.run_until_idle()
        self.assertEqual([r.reply.payload for r in (one, two, other)], [{"echo": 1}, {"echo": 2}, {"echo": 3}])

    def test_on_reply_is_called_from_the_dispatch_loop(self):
        bus = self.bus()
        self._responder(bus)
        replies = []
        pending = request(
            bus, "svc.orders", 7, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1", on_reply=replies.append
        )
        self.assertEqual(replies, [])
        bus.run_until_idle()
        self.assertEqual([r.payload for r in replies], [{"echo": 7}])
        self.assertIs(replies[0], pending.reply)

    def test_a_failing_on_reply_is_retried_by_the_bus(self):
        bus = self.bus()
        self._responder(bus)
        calls = []

        def on_reply(envelope):
            calls.append(envelope.msg_id)
            if len(calls) == 1:
                raise RuntimeError("transient")

        pending = request(
            bus, "svc.orders", 7, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1", on_reply=on_reply
        )
        bus.run_until_idle()
        self.assertEqual(calls, ["strat:1:reply"] * 2)
        self.assertTrue(pending.done)

    def test_the_request_keeps_the_receipt_of_its_latest_send(self):
        bus = self.bus()
        pending = request(bus, "svc.orders", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1")
        first = pending.receipt
        self.assertEqual(first.msg_id, "strat:1")
        again = pending.send()
        self.assertIs(pending.receipt, again)
        self.assertNotEqual(again.position, first.position)

    def _leaked(self, bus, topic="reply.strat"):
        """Deliveries a message to `topic` causes: nonzero if a request left its subscription behind."""
        publish(bus, topic)
        return bus.run_until_idle()

    def test_a_request_that_cannot_be_published_leaves_no_subscription(self):
        bus = self.bus()
        for bad in (dict(logical_ts=1.5), dict(topic="svc.*")):
            arguments = dict(topic="svc.orders", payload=1, type="t", logical_ts=1)
            arguments.update(bad)
            with self.assertRaises((TypeError, ValueError)):
                request(bus, arguments.pop("topic"), arguments.pop("payload"), producer_id="strat", msg_id="strat:1", **arguments)
        self.assertEqual(self._leaked(bus), 0)

    def test_the_reply_topic_must_be_an_exact_topic(self):
        bus = self.bus()
        for reply_to in ("reply.*", "reply.>", "", "a..b", ".a"):
            with self.assertRaises(ValueError):
                request(bus, "svc", 1, type="t", logical_ts=1, producer_id="strat", msg_id="m", reply_to=reply_to)
        with self.assertRaises(ValueError):
            request(bus, "svc", 1, type="t", logical_ts=1, producer_id="*", msg_id="m")
        self.assertEqual(self._leaked(bus, "reply.x"), 0)

    def test_reply_carries_phase_and_schema_version(self):
        bus = self.bus()

        def respond(delivery):
            reply(bus, delivery, 1, type="t", logical_ts=5, producer_id="svc", phase=2, schema_version=4)

        bus.subscribe("svc", respond)
        pending = request(bus, "svc", 1, type="t", logical_ts=1, producer_id="strat", msg_id="strat:1", phase=1)
        bus.run_until_idle()
        self.assertEqual((pending.reply.phase, pending.reply.schema_version, pending.reply.logical_ts), (2, 4, 5))

    # S14 quiescence

    def test_run_until_idle_drains_cascades_and_counts_invocations(self):
        bus = self.bus()
        bus.subscribe("a", lambda d: publish(bus, "b", ts=d.envelope.logical_ts + 1))
        bus.subscribe("b", lambda d: publish(bus, "c", ts=d.envelope.logical_ts + 1))
        bus.subscribe("c", self.rec.handler("end"))
        publish(bus, "a")
        publish(bus, "a", ts=100)
        self.assertEqual(bus.run_until_idle(), 6)
        self.assertEqual(len(self.rec.calls), 2)
        self.assertEqual(bus.run_until_idle(), 0)
        self.assertFalse(bus.step())

    def test_step_runs_exactly_one_handler(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("a"))
        bus.subscribe("t", self.rec.handler("b"))
        publish(bus, "t")
        self.assertTrue(bus.step())
        self.assertEqual(self.rec.names(), ["a"])
        self.assertTrue(bus.step())
        self.assertFalse(bus.step())
        self.assertEqual(self.rec.names(), ["a", "b"])

    def test_an_empty_bus_is_idle(self):
        bus = self.bus()
        self.assertEqual(bus.run_until_idle(), 0)
        publish(bus, "nobody-listens")
        self.assertEqual(bus.run_until_idle(), 0)

    def test_poll_reads_the_log_and_never_runs_a_handler(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t")
        self.assertIsInstance(bus.networked, bool)
        self.assertIsInstance(bus.poll(0.0), bool)
        self.assertEqual(self.rec.calls, [])
        self.assertEqual(bus.run_until_idle(), 1)

    def test_a_bus_nobody_else_writes_to_has_nothing_to_poll(self):
        bus = self.bus()
        if not bus.networked:
            self.assertFalse(bus.poll(30.0))  # at once, the test would not end otherwise

    def test_poll_finds_a_message_published_by_another_bus_of_the_same_log(self):
        bus = self.bus()
        other = getattr(self, "attach", lambda bus: None)(bus)
        if other is None:
            self.skipTest("a bus that is not networked has no other writer")
        self.addCleanup(other.close)
        bus.subscribe("t", self.rec.handler("s"))
        bus.run_until_idle()
        publish(other, "t", msg_id="from-other")
        self.assertTrue(bus.poll(5.0))
        self.assertEqual(self.rec.calls, [])
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["from-other"])
        self.assertFalse(bus.poll(0.0))

    def test_poll_after_close_raises(self):
        bus = type(self).make_bus()
        bus.close()
        with self.assertRaises(BusClosed):
            bus.poll()

    def test_max_deliveries_stops_a_handler_loop(self):
        bus = self.bus()
        bus.subscribe("ping", lambda d: publish(bus, "pong"))
        bus.subscribe("pong", lambda d: publish(bus, "ping"))
        publish(bus, "ping")
        with self.assertRaises(NotIdle):
            bus.run_until_idle(max_deliveries=50)

    def test_max_deliveries_equal_to_the_work_is_not_an_error(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t")
        publish(bus, "t")
        self.assertEqual(bus.run_until_idle(max_deliveries=2), 2)

    def test_max_deliveries_zero_and_invalid_values(self):
        bus = self.bus()
        self.assertEqual(bus.run_until_idle(max_deliveries=0), 0)  # idle
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t")
        with self.assertRaises(NotIdle):
            bus.run_until_idle(max_deliveries=0)
        for bad in (-1, 1.5, True, "3"):
            with self.assertRaises(ValueError):
                bus.run_until_idle(max_deliveries=bad)
        self.assertEqual(self.rec.calls, [])
        self.assertEqual(bus.run_until_idle(), 1)

    def test_the_nesting_check_comes_before_the_limit_check(self):
        bus = self.bus()
        errors = []

        def handler(delivery):
            try:
                bus.run_until_idle(max_deliveries=0)
            except BusError as error:
                errors.append(error)

        bus.subscribe("t", handler)
        publish(bus, "t", key="a")
        publish(bus, "t", key="b")
        bus.run_until_idle()
        self.assertEqual([type(error) for error in errors], [BusError, BusError])

    def test_publishing_after_idle_is_dispatched_by_the_next_run(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", msg_id="one")
        bus.run_until_idle()
        publish(bus, "t", msg_id="two")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["one", "two"])

    def test_close_is_idempotent_and_final(self):
        bus = type(self).make_bus()
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t")
        bus.close()
        bus.close()
        self.assertEqual(self.rec.calls, [])
        for call in (
            lambda: publish(bus, "t"),
            lambda: bus.subscribe("t", lambda d: None),
            bus.step,
            bus.run_until_idle,
        ):
            with self.assertRaises(BusClosed):
                call()

    # subscriptions, patterns, groups

    def test_wildcards(self):
        bus = self.bus()
        bus.subscribe("a.*", self.rec.handler("one"))
        bus.subscribe("a.>", self.rec.handler("rest"))
        bus.subscribe("a.b", self.rec.handler("exact"))
        for topic in ("a", "a.b", "a.b.c", "x.b"):
            publish(bus, topic, msg_id=topic)
        bus.run_until_idle()
        self.assertEqual(
            self.rec.calls,
            [("one", "a.b", 1), ("rest", "a.b", 1), ("exact", "a.b", 1), ("rest", "a.b.c", 1)],
        )

    def test_malformed_topics_are_refused(self):
        bus = self.bus()
        for pattern in ("", "a..b", ".a", ">.a"):
            with self.assertRaises(ValueError):
                bus.subscribe(pattern, lambda d: None)
        for topic in ("a.*", "a.>", "*"):
            with self.assertRaises(ValueError):
                publish(bus, topic)

    def test_topics_with_whitespace_or_control_characters_are_refused_by_every_bus(self):
        # the grammar is the one a broker can carry (NATS subjects have no whitespace): a topic that works in memory and
        # not on a networked bus would be a run that works in one mode only, and on a log-backed bus a message refused
        # after publish returned
        bus = self.bus()
        for topic in ("a b", "a\tb", "a.b\n", "a\x00b", " a", "a."):
            with self.assertRaises(ValueError):
                publish(bus, topic)
            with self.assertRaises(ValueError):
                bus.subscribe(topic, lambda d: None)
        publish(bus, "a.b")  # and the bus is as it was
        self.assertEqual(bus.run_until_idle(), 0)

    def test_cancel_drops_queued_messages_and_is_idempotent(self):
        bus = self.bus()
        subscription = bus.subscribe("t", self.rec.handler("gone"))
        bus.subscribe("t", self.rec.handler("kept"))
        publish(bus, "t")
        subscription.cancel()
        subscription.cancel()
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["kept", "kept"])

    def test_a_handler_can_cancel_its_own_subscription(self):
        bus = self.bus()
        holder = []
        holder.append(bus.subscribe("t", self.rec.handler("once", lambda d: holder[0].cancel())))
        publish(bus, "t")
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["once"])

    def test_group_members_share_one_stream(self):
        bus = self.bus()
        bus.subscribe("t.*", self.rec.handler("g1"), group="g")
        bus.subscribe("t.*", self.rec.handler("g2"), group="g")
        bus.subscribe("t.*", self.rec.handler("solo"))
        for i in range(24):
            publish(bus, "t.x", i, key="k%d" % (i % 12), ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        group_calls = [call for call in self.rec.calls if call[0] != "solo"]
        self.assertEqual(sorted(call[1] for call in group_calls), sorted("m%d" % i for i in range(24)))
        self.assertEqual(len(self.rec.ids("solo")), 24)
        owners = {}
        for name, msg_id, _ in group_calls:
            owners.setdefault(int(msg_id[1:]) % 12, set()).add(name)
        self.assertTrue(all(len(names) == 1 for names in owners.values()))  # a key stays with one member

    def test_group_members_must_read_the_same_stream(self):
        bus = self.bus()
        bus.subscribe("t.*", lambda d: None, group="g")
        with self.assertRaises(ValueError):
            bus.subscribe("t.>", lambda d: None, group="g")
        with self.assertRaises(ValueError):
            bus.subscribe("t.*", lambda d: None, group="g", start=Start.EARLIEST)

    def test_cancelling_a_group_member_hands_its_keys_to_the_others(self):
        bus = self.bus()
        first = bus.subscribe("t", self.rec.handler("g1"), group="g")
        bus.subscribe("t", self.rec.handler("g2"), group="g")
        for i in range(4):
            publish(bus, "t", i, key="k%d" % i, ts=i, msg_id="m%d" % i)
        first.cancel()
        bus.run_until_idle()
        self.assertEqual(sorted(self.rec.ids()), ["m0", "m1", "m2", "m3"])
        self.assertEqual(set(self.rec.names()), {"g2"})
        publish(bus, "t", 4, key="k0", ts=9, msg_id="m4")
        bus.run_until_idle()
        self.assertEqual(self.rec.calls[-1], ("g2", "m4", 1))

    def test_cancelling_the_last_group_member_frees_the_group_name(self):
        bus = self.bus()
        bus.subscribe("t", lambda d: None, group="g").cancel()
        bus.subscribe("u", self.rec.handler("again"), group="g", start=Start.LATEST)
        publish(bus, "u")
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["again"])

    # envelope and publish

    def test_envelope_fields_and_defaults(self):
        bus = self.bus()
        seen = []
        bus.subscribe("t", seen.append)
        first = publish(bus, "t", {"x": 1}, key="k", ts=42, producer="p1", correlation_id="c1", reply_to="r1", schema_version=3)
        second = publish(bus, "t", ts=100, producer="p1")
        third = publish(bus, "t", ts=100, producer="p2")
        fixed = publish(bus, "t", ts=100, producer="p1", seq=99, msg_id="mine")
        bus.run_until_idle()
        self.assertEqual([r.msg_id for r in (first, second, third, fixed)], ["p1:1", "p1:2", "p2:1", "mine"])
        by_id = {d.envelope.msg_id: d.envelope for d in seen}
        self.assertEqual(
            tuple(by_id["p1:1"]),
            ("p1:1", "t", "k", "t", 42, 0, "p1", 1, "c1", "r1", 3, {"x": 1}),
        )
        later = by_id["mine"]
        self.assertEqual((later.seq, later.schema_version, later.correlation_id), (99, 1, None))
        self.assertEqual(
            [r.position.topic for r in (first, second)], ["t", "t"]
        )

    def test_envelopes_are_immutable(self):
        bus = self.bus()
        seen = []
        bus.subscribe("t", seen.append)
        publish(bus, "t", 1)
        bus.run_until_idle()
        with self.assertRaises(AttributeError):
            seen[0].envelope.payload = 2
        with self.assertRaises(AttributeError):
            seen[0].attempt = 2

    def test_logical_time_must_be_a_plain_int(self):
        bus = self.bus()
        for ts in (1.5, "1", None, True):
            with self.assertRaises(TypeError):
                publish(bus, "t", ts=ts)

    def test_protobuf_payloads_arrive_equal(self):
        bus = self.bus()
        seen = []
        bus.subscribe("t", seen.append)
        message = Timestamp(seconds=1_700_000_000, nanos=5)
        bus.publish("t", message, type="timestamp", logical_ts=1, producer_id="p")
        bus.run_until_idle()
        self.assertEqual(seen[0].envelope.payload, message)

    def test_published_message_goes_to_subscribers_registered_before_it_only(self):
        bus = self.bus()
        publish(bus, "t", msg_id="before")
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", msg_id="after")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["after"])

    def test_a_refused_publish_changes_nothing(self):
        bus = self.bus()
        for bad in (dict(ts=1.5), dict(ts=True), dict(phase="1"), dict(seq="1"), dict(key=None), dict(producer=7)):
            with self.assertRaises(TypeError):
                publish(bus, "t", **bad)
        with self.assertRaises(ValueError):
            publish(bus, "t.*")
        self.assertEqual(publish(bus, "t").msg_id, "p:1")  # the refused calls did not use up a seq
        seen = []
        bus.subscribe("t", seen.append, start=Start.EARLIEST)
        bus.run_until_idle()
        self.assertEqual(len(seen), 1)  # nor did they reach the log

    def test_rank_must_be_an_int(self):
        bus = self.bus()
        for rank in ("1", 1.5, float("nan"), True, None):
            with self.assertRaises(TypeError):
                bus.subscribe("t", lambda d: None, rank=rank)

    def test_a_group_accepts_members_that_start_at_latest(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("g1"), group="g")
        bus.subscribe("t", self.rec.handler("g2"), group="g", start=Start.LATEST)
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(len(self.rec.calls), 1)

    def test_a_group_member_cancelling_itself_hands_the_rest_to_the_others(self):
        bus = self.bus()
        holder = []
        holder.append(bus.subscribe("t", self.rec.handler("g1", lambda d: holder[0].cancel()), group="g"))
        bus.subscribe("t", self.rec.handler("g2"), group="g")
        for i in range(8):
            publish(bus, "t", i, key="k%d" % i, ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        self.assertEqual(sorted(self.rec.ids()), sorted("m%d" % i for i in range(8)))
        self.assertLessEqual(self.rec.names().count("g1"), 1)

    def test_a_group_member_that_nacks_and_cancels_hands_the_retry_to_the_others(self):
        bus = self.bus()
        holder = []

        def nack_and_leave(delivery):
            bus.nack(delivery)
            holder[0].cancel()

        holder.append(bus.subscribe("t", self.rec.handler("g1", nack_and_leave), group="g"))
        bus.subscribe("t", self.rec.handler("g2"), group="g")
        for i in range(8):
            publish(bus, "t", i, key="k%d" % i, ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        retried = [call for call in self.rec.calls if call[2] == 2]
        first_member = [call for call in self.rec.calls if call[0] == "g1"]
        self.assertEqual(len(first_member), len(retried))
        self.assertTrue(all(call[0] == "g2" for call in retried))
        self.assertEqual(sorted({call[1] for call in self.rec.calls}), sorted("m%d" % i for i in range(8)))

    def test_a_handler_can_cancel_another_subscription_that_has_messages_queued(self):
        bus = self.bus()
        victim = bus.subscribe("t", self.rec.handler("victim"), rank=1)
        bus.subscribe("t", self.rec.handler("killer", lambda d: victim.cancel()), rank=0)
        publish(bus, "t")
        publish(bus, "t", ts=1)
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["killer", "killer"])

    # closing

    def test_cancel_after_close_is_a_no_op(self):
        bus = type(self).make_bus()
        single = bus.subscribe("t", lambda d: None)
        member = bus.subscribe("t", lambda d: None, group="g")
        pending = request(bus, "svc", 1, type="t", logical_ts=0, producer_id="p", msg_id="p:1")
        bus.close()
        single.cancel()
        member.cancel()
        pending.cancel()

    def test_a_handler_that_closes_the_bus_leaves_nothing_behind(self):
        bus = type(self).make_bus(max_attempts=1)

        def closing(delivery):
            self.rec.calls.append(("closing", delivery.envelope.msg_id, delivery.attempt))
            bus.close()
            raise RuntimeError("after close")

        bus.subscribe("t", closing)
        bus.subscribe("t", self.rec.handler("other"), rank=1)
        publish(bus, "t")
        publish(bus, "t", ts=1)
        self.assertTrue(bus.step())
        self.assertEqual(self.rec.names(), ["closing"])
        with self.assertRaises(BusClosed):
            bus.step()

    def test_a_closed_bus_refuses_every_call(self):
        bus = type(self).make_bus()
        seen = []
        bus.subscribe("t", seen.append)
        publish(bus, "t")
        bus.step()
        bus.close()
        for call in (
            lambda: bus.position_of(seen[0]),
            lambda: bus.ack(seen[0]),
            lambda: bus.nack(seen[0]),
            bus.dead_letters,
        ):
            with self.assertRaises(BusClosed):
                call()


def _fake_delivery():
    return Delivery(Envelope("x", "t", "", "t", 0, 0, "p", 0), 1, 1, 0)


class InMemoryBusContract(BusContract, unittest.TestCase):
    implementation = "memory"
    make_bus = staticmethod(make_memory)


class SerializingBusContract(BusContract, unittest.TestCase):
    implementation = "serializing"
    make_bus = staticmethod(make_serializing)


@nats_support.needs_server
class JetStreamBusContract(BusContract, unittest.TestCase):
    """The same contract over the log-backed bus on a NATS JetStream stream (one new stream per bus, deleted on close)."""

    implementation = "nats"
    make_bus = staticmethod(make_nats)

    def attach(self, bus):
        """Another bus on the same log, as another process would build it."""
        namespace = bus._inner._log.namespace
        return SerializingBus(
            LogBus(nats_support.jetstream.JetStreamLog(namespace, create=False), max_attempts=3),
            ProtobufCodec({"timestamp": Timestamp}),
        )


if __name__ == "__main__":
    json.dump(scenario_trace(FACTORIES[sys.argv[1]]), sys.stdout)
