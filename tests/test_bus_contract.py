"""The bus contract (pluto/bus/__init__.py, rules S1 to S14), run on every implementation.

`BusContract` holds the tests; one subclass per implementation supplies `make_bus`. Payloads are JSON-compatible
(or protobuf Timestamps) so that the same assertions hold with serialisation on and off.

Run as `python -m tests.test_bus_contract <implementation>` it prints the delivery trace of a fixed scenario,
which the hash-seed test compares across interpreters started with different `PYTHONHASHSEED` values.
"""
import dataclasses
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
    Position,
    ProtobufCodec,
    QueueFull,
    SerializingBus,
    Start,
    reply,
    request,
)

REPO = Path(__file__).resolve().parent.parent
HASH_SEEDS = ("1", "2", "3", "4", "5")


def make_memory(**options):
    return InMemoryBus(**options)


def make_serializing(**options):
    return SerializingBus(InMemoryBus(**options), ProtobufCodec({"timestamp": Timestamp}))


FACTORIES = {"memory": make_memory, "serializing": make_serializing}


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
    """A fixed workload that exercises every ordering path: ties, wildcards, ranks, a group, failures, cascades."""
    bus = make_bus(max_attempts=3)
    rec = Recorder()
    rng = random.Random(1234)

    def flaky(delivery):
        if delivery.envelope.seq % 7 == 0 and delivery.attempt == 1:
            raise RuntimeError("transient")

    def follow_up(delivery):
        env = delivery.envelope
        if env.seq % 5 == 0 and env.topic.startswith("orders."):
            publish(bus, "fills." + env.topic[7:], env.payload, key=env.key, ts=env.logical_ts, producer="fill-" + env.key)

    bus.subscribe("orders.*", rec.handler("orders", follow_up), rank=1)
    bus.subscribe("orders.a", rec.handler("orders-a", flaky), rank=0)
    bus.subscribe("fills.>", rec.handler("fills"))
    bus.subscribe(">", rec.handler("all"), rank=5, start=Start.LATEST)
    bus.subscribe("orders.*", rec.handler("g1"), group="g")
    bus.subscribe("orders.*", rec.handler("g2"), group="g")
    for i in range(200):
        topic = ("orders.a", "orders.b", "orders.c")[rng.randrange(3)]
        publish(
            bus,
            topic,
            i,
            key="k%d" % rng.randrange(6),
            ts=rng.randrange(10),
            producer="prod-%d" % rng.randrange(4),
        )
        if i % 40 == 39:
            bus.run_until_idle()
    bus.run_until_idle()
    late = Recorder()
    bus.subscribe("fills.>", late.handler("late"), start=Start.EARLIEST)
    bus.run_until_idle()
    return {"calls": rec.calls, "late": late.calls, "dead": [(d.envelope.msg_id, d.attempts) for d in bus.dead_letters()]}


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

    def test_equal_content_in_one_subscription_falls_back_to_lane_creation_order(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"))
        for key in ("k2", "k1", "k3"):
            publish(bus, "t", key=key, ts=1, seq=1, msg_id="m-" + key)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m-k2", "m-k1", "m-k3"])

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
        self.assertIs(letter.error, error)

    def test_explicit_nack_dead_letter_has_no_error(self):
        bus = self.bus(max_attempts=2)
        bus.subscribe("t", lambda d: bus.nack(d))
        publish(bus, "t")
        bus.run_until_idle()
        (letter,) = bus.dead_letters()
        self.assertIsNone(letter.error)
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

    def test_position_needs_its_exact_topic_and_a_place_in_the_log(self):
        bus, receipts = self._filled(2)
        position = receipts[0].position
        with self.assertRaises(ValueError):
            bus.subscribe("t.*", lambda d: None, start=position)
        with self.assertRaises(ValueError):
            bus.subscribe("other", lambda d: None, start=position)
        with self.assertRaises(ValueError):
            bus.subscribe("t", lambda d: None, start=Position("t", 3))
        with self.assertRaises(ValueError):
            bus.subscribe("t", lambda d: None, start=Position("t", -1))

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

    # S12 backpressure

    def test_overflow_raises_to_the_publisher_and_changes_nothing(self):
        bus = self.bus(queue_capacity=3)
        bus.subscribe("t", self.rec.handler("s"))
        for i in range(3):
            publish(bus, "t", i, ts=i, msg_id="m%d" % i)
        with self.assertRaises(QueueFull):
            publish(bus, "t", 3, msg_id="m3")
        self.assertEqual(self.rec.calls, [])
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m0", "m1", "m2"])
        receipt = publish(bus, "t", 4, ts=9, msg_id="m4")
        self.assertEqual(receipt.position, Position("t", 3))  # the refused message was never logged
        bus.run_until_idle()
        self.assertEqual(self.rec.ids()[-1], "m4")

    def test_overflow_in_one_subscription_refuses_the_message_for_all(self):
        bus = self.bus(queue_capacity=2)
        bus.subscribe("a.*", self.rec.handler("wide"))
        bus.subscribe("a.y", self.rec.handler("narrow"))
        publish(bus, "a.x")
        publish(bus, "a.x")
        with self.assertRaises(QueueFull):
            publish(bus, "a.y")
        bus.run_until_idle()
        self.assertEqual(self.rec.names(), ["wide", "wide"])

    def test_capacity_counts_per_subscription_and_frees_on_ack(self):
        bus = self.bus(queue_capacity=1)
        bus.subscribe("t", self.rec.handler("one"))
        bus.subscribe("t", self.rec.handler("two"))
        for i in range(5):
            publish(bus, "t", i, ts=i)
            bus.run_until_idle()
        self.assertEqual(len(self.rec.calls), 10)

    def test_a_handler_that_overflows_is_nacked_not_raised(self):
        bus = self.bus(queue_capacity=2, max_attempts=2)
        errors = []

        def handler(delivery):
            try:
                publish(bus, "out", delivery.envelope.payload, ts=10)
            except QueueFull as error:
                errors.append(error)
                raise

        bus.subscribe("t", handler)
        bus.subscribe("out", self.rec.handler("sink"), rank=1)
        publish(bus, "t", 0, ts=0)
        publish(bus, "t", 1, ts=1)
        bus.step()
        bus.step()  # the sink's queue now holds two messages
        publish(bus, "t", 2, ts=2)
        bus.run_until_idle()
        self.assertEqual(len(errors), 2)  # both attempts at the third message found the sink full
        self.assertEqual(self.rec.names(), ["sink", "sink"])
        (letter,) = bus.dead_letters()
        self.assertEqual((letter.envelope.payload, letter.attempts), (2, 2))
        self.assertIsInstance(letter.error, QueueFull)

    def test_a_replay_larger_than_the_capacity_is_refused_and_registers_nothing(self):
        bus = self.bus(queue_capacity=2)
        bus.subscribe("t", lambda d: None)
        for i in range(3):
            publish(bus, "t", i)
            bus.run_until_idle()
        with self.assertRaises(QueueFull):
            bus.subscribe("t", self.rec.handler("s"), start=Start.EARLIEST)
        publish(bus, "t", 3)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])

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
        for i in range(12):
            publish(bus, "t.x", i, key="k%d" % (i % 4), ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        group_calls = [call for call in self.rec.calls if call[0] != "solo"]
        self.assertEqual(sorted(call[1] for call in group_calls), sorted("m%d" % i for i in range(12)))
        self.assertEqual(len(self.rec.ids("solo")), 12)
        self.assertEqual(set(self.rec.names()), {"g1", "g2", "solo"})
        owners = {}
        for name, msg_id, _ in group_calls:
            owners.setdefault(int(msg_id[1:]) % 4, set()).add(name)
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
            dataclasses.astuple(by_id["p1:1"]),
            ("p1:1", "t", "k", "t", 42, "p1", 1, "c1", "r1", 3, {"x": 1}),
        )
        later = by_id["mine"]
        self.assertEqual((later.seq, later.schema_version, later.correlation_id), (99, 1, None))
        self.assertEqual([r.position for r in (first, second)], [Position("t", 0), Position("t", 1)])

    def test_envelopes_are_immutable(self):
        bus = self.bus()
        seen = []
        bus.subscribe("t", seen.append)
        publish(bus, "t", 1)
        bus.run_until_idle()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            seen[0].envelope.payload = 2
        with self.assertRaises(dataclasses.FrozenInstanceError):
            seen[0].attempt = 2

    def test_logical_time_must_be_integer_nanoseconds(self):
        bus = self.bus()
        for ts in (1.5, "1", None):
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


def _fake_delivery():
    return Delivery(Envelope("x", "t", "", "t", 0, "p", 0), 1, 1, 0)


class InMemoryBusContract(BusContract, unittest.TestCase):
    implementation = "memory"
    make_bus = staticmethod(make_memory)


class SerializingBusContract(BusContract, unittest.TestCase):
    implementation = "serializing"
    make_bus = staticmethod(make_serializing)


if __name__ == "__main__":
    json.dump(scenario_trace(FACTORIES[sys.argv[1]]), sys.stdout)
