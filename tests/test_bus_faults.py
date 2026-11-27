"""Tests of `pluto.bus.faults.SeededFaultBus`: reproducible faults over `InMemoryBus` and `SerializingBus(InMemoryBus)`.

What is tested: with no faults the wrapper is transparent (the whole broker-neutral contract passes over it); with
faults the same seed gives the same trace under any `PYTHONHASHSEED` and different seeds give different ones; per-lane
FIFO holds through every fault the contract allows and fails under the contract-violating one; every fault has a
focused test. `tests/test_bus_faults_contract.py` runs the contract suite with faults on and lists what it may not
rely on.

Run as `python -m tests.test_bus_faults <implementation>` it prints the trace of the fault scenario, as
`python -m tests.test_bus_faults transparent <implementation>` that of the contract scenario over the wrapper without
faults; the hash-seed tests compare these across interpreters.
"""

import json
import os
import random
import subprocess
import sys
import unittest

from pluto.bus import BusClosed, BusError, NotIdle, Position, QueueFull, Start, reply, request
from pluto.bus.faults import FaultConfig, FaultKind, PublishFailed, SeededFaultBus
from tests.test_bus_contract import (
    HASH_SEEDS,
    REPO,
    BusContract,
    Recorder,
    make_memory,
    make_serializing,
    publish,
    scenario_trace,
)

FACTORIES = {"memory": make_memory, "serializing": make_serializing}

# every fault the contract allows, at rates that hit a 120 message workload many times
CHAOS = FaultConfig(
    publish_fail=0.1,
    publish_ack_lost=0.05,
    delay=0.3,
    duplicate_publish=0.1,
    max_delay=6,
    lose_delivery=0.1,
    ack_lost=0.1,
    crash=0.05,
)
SCENARIO_SEED = 11


def faulty(implementation, seed, faults=None, **options):
    """A fault bus over the named implementation; `options` are those of the inner bus."""
    return SeededFaultBus(FACTORIES[implementation](**options), seed, faults)


def transparent(implementation):
    return lambda **options: faulty(implementation, 0, None, **options)


def fault_scenario(make_bus):
    """A workload over exact topics, a pattern and a group, with handlers that publish follow-ups. Publishers retry a
    failed publish with the same `msg_id`. `sent` lists the messages in the order they were first published."""
    bus = make_bus(max_attempts=6)
    calls, sent = [], []
    rng = random.Random(77)

    def send(topic, key, payload, ts, producer, msg_id, phase=0):
        sent.append((topic, key, msg_id))
        for _ in range(100):
            try:
                bus.publish(
                    topic, payload, type="t", logical_ts=ts, producer_id=producer, key=key, phase=phase, msg_id=msg_id
                )
                return
            except PublishFailed:
                pass
        raise AssertionError("publish kept failing")

    def record(name, follow=False):
        def handle(delivery):
            e = delivery.envelope
            calls.append(
                [name, e.msg_id, e.topic, e.key, e.logical_ts, e.phase, e.producer_id, e.seq, e.payload, delivery.attempt]
            )
            if follow and e.topic.startswith("orders.") and e.payload % 5 == 0:
                send("fills." + e.topic[7:], e.key, e.payload, e.logical_ts, "fill-" + e.key, "fill-" + e.msg_id)

        return handle

    bus.subscribe("orders.a", record("a", True))
    bus.subscribe("orders.b", record("b", True), rank=1)
    bus.subscribe("orders.*", record("any"), rank=2)
    bus.subscribe("fills.>", record("fills"))
    bus.subscribe("orders.c", record("g1"), group="g")
    bus.subscribe("orders.c", record("g2"), group="g")
    for i in range(120):
        send(
            "orders." + "abc"[rng.randrange(3)],
            "k%d" % rng.randrange(4),
            i,
            rng.randrange(10),
            "prod-%d" % rng.randrange(3),
            "m%d" % i,
            phase=rng.randrange(3),
        )
        if i % 30 == 29:
            bus.run_until_idle()
    bus.run_until_idle()
    return {
        "calls": calls,
        "sent": sent,
        "faults": [list(fault) for fault in bus.fault_log()],
        "dead": [(d.envelope.msg_id, d.attempts) for d in bus.dead_letters()],
    }


def first_seen(calls):
    """For each handler and lane, the msg_ids in the order of their first delivery."""
    seen = {}
    for name, msg_id, topic, key, *_ in calls:
        ids = seen.setdefault((name, topic, key), [])
        if msg_id not in ids:
            ids.append(msg_id)
    return seen


def lane_fifo_violations(result):
    """The (handler, topic, key) lanes in which a message was first delivered before one published earlier."""
    position = {}
    for index, (topic, key, msg_id) in enumerate(result["sent"]):
        position.setdefault(msg_id, index)
    bad = []
    for lane, ids in first_seen(result["calls"]).items():
        indexes = [position[msg_id] for msg_id in ids]
        if indexes != sorted(indexes):
            bad.append(lane)
    return bad


def run_subprocess(*args, hash_seed):
    env = dict(os.environ, PYTHONHASHSEED=hash_seed)
    out = subprocess.run(
        [sys.executable, "-m", "tests.test_bus_faults", *args],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return json.loads(out)


class FaultBusTests:
    implementation = None

    def setUp(self):
        self.rec = Recorder()

    def bus(self, seed=0, faults=None, **options):
        bus = faulty(self.implementation, seed, faults, **options)
        self.addCleanup(bus.close)
        return bus

    def scenario(self, seed, faults=CHAOS):
        bus_factory = lambda **options: faulty(self.implementation, seed, faults, **options)  # noqa: E731
        return json.loads(json.dumps(fault_scenario(bus_factory)))

    # the traces

    def test_the_same_seed_gives_the_same_trace(self):
        first = self.scenario(SCENARIO_SEED)
        second = self.scenario(SCENARIO_SEED)
        self.assertEqual(first, second)
        kinds = {fault[2] for fault in first["faults"]}
        self.assertEqual(
            kinds,
            {
                "publish_failed",
                "publish_ack_lost",
                "delayed",
                "queued_behind",
                "duplicate_publish",
                "lost_delivery",
                "duplicate_delivery",
                "crash",
            },
        )
        self.assertGreater(len(first["calls"]), 300)

    def test_different_seeds_give_different_traces(self):
        traces = {json.dumps(self.scenario(seed)["calls"]) for seed in range(8)}
        self.assertEqual(len(traces), 8)
        logs = {json.dumps(self.scenario(seed)["faults"]) for seed in range(8)}
        self.assertEqual(len(logs), 8)

    def test_the_trace_is_independent_of_hash_seed(self):
        expected = self.scenario(SCENARIO_SEED)
        for hash_seed in HASH_SEEDS:
            self.assertEqual(
                run_subprocess(self.implementation, hash_seed=hash_seed), expected, "PYTHONHASHSEED=" + hash_seed
            )

    # what always holds

    def test_lane_fifo_holds_through_every_allowed_fault(self):
        for seed in range(25):
            self.assertEqual(lane_fifo_violations(self.scenario(seed)), [], "seed %d" % seed)

    def test_the_fifo_check_catches_the_contract_violating_fault(self):
        violating = FaultConfig(delay=0.5, max_delay=6, reorder_within_lane=0.5, contract_violating=True)
        bad = [seed for seed in range(10) if lane_fifo_violations(self.scenario(seed, violating))]
        self.assertGreaterEqual(len(bad), 8)

    def test_a_duplicate_carries_the_same_envelope_and_msg_id(self):
        repeated = 0
        for seed in range(10):
            by_id = {}
            for call in self.scenario(seed)["calls"]:
                name, msg_id, topic, key, ts, phase, producer, seq, payload, attempt = call
                # the retry of a failed publish is a new call of the publisher: same msg_id, next seq
                by_id.setdefault((name, msg_id), []).append([topic, key, ts, phase, producer, payload])
            for deliveries in by_id.values():
                self.assertTrue(all(envelope == deliveries[0] for envelope in deliveries))
                repeated += len(deliveries) > 1
        self.assertGreater(repeated, 100)

    def test_without_faults_the_wrapper_adds_nothing(self):
        plain = scenario_trace(FACTORIES[self.implementation])
        wrapped = scenario_trace(transparent(self.implementation))
        self.assertEqual(wrapped, plain)
        bus = faulty(self.implementation, 5)
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(bus.fault_log(), ())
        bus.close()

    # one test per fault

    def test_a_failed_publish_reaches_nobody_and_is_retried_with_the_same_msg_id(self):
        bus = self.bus(3, FaultConfig(publish_fail=0.5))
        for i in range(10):
            for _ in range(50):
                try:
                    publish(bus, "t", i, msg_id="m%d" % i)
                    break
                except PublishFailed:
                    pass
        bus.run_until_idle()
        replayed = Recorder()
        bus.subscribe("t", replayed.handler("s"), start=Start.EARLIEST)
        bus.run_until_idle()
        failed = [f for f in bus.fault_log() if f.kind is FaultKind.PUBLISH_FAILED]
        self.assertGreater(len(failed), 2)
        self.assertEqual(replayed.ids(), ["m%d" % i for i in range(10)])  # nothing logged twice, none missing

    def test_a_failed_publish_uses_no_seq(self):
        bus = self.bus(3, FaultConfig(publish_fail=0.5))
        ids = []
        for _ in range(8):
            while True:
                try:
                    ids.append(publish(bus, "t").msg_id)
                    break
                except PublishFailed:
                    pass
        self.assertEqual(ids, ["p:%d" % i for i in range(1, 9)])
        self.assertTrue(bus.fault_log())

    def test_an_ambiguous_publish_reached_the_bus_and_its_retry_is_a_duplicate(self):
        bus = self.bus(4, FaultConfig(publish_ack_lost=0.5))
        for i in range(10):
            for _ in range(50):
                try:
                    publish(bus, "t", i, msg_id="m%d" % i, key="k")
                    break
                except PublishFailed:
                    pass
        bus.run_until_idle()
        seen = Recorder()
        bus.subscribe("t", seen.handler("s"), start=Start.EARLIEST)
        bus.run_until_idle()
        ambiguous = [f for f in bus.fault_log() if f.kind is FaultKind.PUBLISH_ACK_LOST]
        self.assertGreater(len(ambiguous), 2)
        self.assertEqual(sorted(set(seen.ids())), sorted("m%d" % i for i in range(10)))
        self.assertEqual(len(seen.ids()), 10 + len(ambiguous))  # each ambiguous failure became a duplicate
        for fault in ambiguous:
            tries = [f for f in ambiguous if f.msg_id == fault.msg_id]
            self.assertEqual(seen.ids().count(fault.msg_id), 1 + len(tries))

    def test_a_delay_changes_the_interleaving_across_lanes_and_never_inside_one(self):
        def run(seed, faults=None):
            bus = self.bus(seed, faults)
            rec = Recorder()
            bus.subscribe("t.*", rec.handler("s"))
            for i in range(40):
                publish(bus, "t.%d" % (i % 4), i, key="k%d" % (i % 2), ts=i, msg_id="m%d" % i)
            bus.run_until_idle()
            self.assertEqual(bus.in_flight(), 0)
            return rec.ids()

        in_order = run(0)
        self.assertEqual(in_order, ["m%d" % i for i in range(40)])  # S3 order, without faults
        reordered = 0
        for seed in range(10):
            ids = run(seed, FaultConfig(delay=0.5, max_delay=6))
            self.assertEqual(sorted(ids), sorted(in_order))  # nothing lost, nothing doubled
            reordered += ids != in_order
            for lane in range(4):  # each (topic, key) lane in publish order
                for key in range(2):
                    mine = [int(m[1:]) for m in ids if int(m[1:]) % 4 == lane and int(m[1:]) % 2 == key]
                    self.assertEqual(mine, sorted(mine))
        self.assertGreaterEqual(reordered, 8)

    def test_a_message_that_arrives_late_is_dispatched_after_later_ones_whichever_sorts_first(self):
        def run(seed):
            bus = self.bus(seed, FaultConfig(delay=0.5, max_delay=3))
            rec = Recorder()
            bus.subscribe("t", rec.handler("s"))
            publish(bus, "t", key="a", ts=1, msg_id="early")
            publish(bus, "t", key="b", ts=2, msg_id="later")
            bus.run_until_idle()
            return rec.ids()

        orders = {tuple(run(seed)) for seed in range(20)}
        self.assertEqual(orders, {("early", "later"), ("later", "early")})

    def test_a_delayed_message_is_in_the_log_before_a_later_subscription_and_its_receipt_resolves(self):
        bus = self.bus(0, FaultConfig(delay=1.0, max_delay=3))
        before = Recorder()
        bus.subscribe("t", before.handler("before"))
        receipt = publish(bus, "t", 1, msg_id="held")
        self.assertEqual(bus.in_flight(), 1)
        after = Recorder()
        bus.subscribe("t", after.handler("after"))  # a barrier: "held" is in the log, so LATEST skips it
        self.assertEqual(bus.in_flight(), 0)
        publish(bus, "t", 2, msg_id="next", ts=1)
        from_receipt = Recorder()
        bus.subscribe("t", from_receipt.handler("receipt"), start=receipt.position)
        bus.run_until_idle()
        self.assertEqual(before.ids(), ["held", "next"])
        self.assertEqual(after.ids(), ["next"])
        self.assertEqual(from_receipt.ids(), ["held", "next"])
        self.assertEqual(receipt.msg_id, "held")

    def test_the_wrapper_numbers_the_messages_like_the_bus_does(self):
        bus = self.bus(2, FaultConfig(delay=0.5, max_delay=4))
        rec = Recorder()
        bus.subscribe("t", rec.handler("s"))
        ids = [publish(bus, "t", producer="a").msg_id for _ in range(10)] + [publish(bus, "t", producer="b").msg_id]
        self.assertEqual(ids, ["a:%d" % i for i in range(1, 11)] + ["b:1"])
        bus.run_until_idle()
        self.assertEqual(sorted(rec.ids()), sorted(ids))

    def test_arguments_are_checked_when_the_message_is_held(self):
        bus = self.bus(0, FaultConfig(delay=1.0))
        with self.assertRaises(ValueError):
            publish(bus, "a.*")
        with self.assertRaises(TypeError):
            publish(bus, "t", ts=1.5)
        self.assertEqual(bus.in_flight(), 0)
        self.assertEqual(publish(bus, "t").msg_id, "p:1")  # the refused calls used no seq

    def test_a_late_duplicate_has_the_same_envelope_and_comes_after_later_messages_of_its_lane(self):
        late = False
        for seed in range(10):
            bus = self.bus(seed, FaultConfig(duplicate_publish=1.0, max_delay=4))
            seen = []
            bus.subscribe("t", lambda d: seen.append(d.envelope))
            for i in range(6):
                publish(bus, "t", i, key="k", ts=i, msg_id="m%d" % i)
                bus.step()
            bus.run_until_idle()
            self.assertEqual(sorted(e.msg_id for e in seen), sorted(["m%d" % i for i in range(6)] * 2))
            for msg_id in {e.msg_id for e in seen}:
                copies = [e for e in seen if e.msg_id == msg_id]
                self.assertEqual(copies[0], copies[1])  # the same envelope: payload, seq, everything
            firsts = [e.msg_id for i, e in enumerate(seen) if e.msg_id not in [x.msg_id for x in seen[:i]]]
            self.assertEqual(firsts, ["m%d" % i for i in range(6)])
            ids = [int(e.msg_id[1:]) for e in seen]
            late |= any(ids[k] < max(ids[:k]) for k in range(1, len(ids)))
        self.assertTrue(late)

    def test_a_lost_ack_delivers_the_message_again_before_the_next_of_its_lane(self):
        bus = self.bus(0, FaultConfig(ack_lost=1.0))
        bus.subscribe("t", self.rec.handler("s"))
        for i in range(3):
            publish(bus, "t", i, msg_id="m%d" % i, key="k", ts=i)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("s", m, a) for m in ("m0", "m1", "m2") for a in (1, 2)])
        self.assertEqual(bus.dead_letters(), ())
        self.assertEqual([f.kind for f in bus.fault_log()], [FaultKind.DUPLICATE_DELIVERY] * 3)

    def test_max_redeliveries_bounds_the_injection_per_message(self):
        bus = self.bus(0, FaultConfig(ack_lost=1.0, lose_delivery=1.0, max_redeliveries=2), max_attempts=4)
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", msg_id="m0")
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("s", "m0", 3)])  # lost twice, then handled; the ack is not lost again
        self.assertEqual(bus.dead_letters(), ())

    def test_a_lost_delivery_comes_again_and_too_many_end_in_a_dead_letter(self):
        bus = self.bus(0, FaultConfig(lose_delivery=1.0))
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", msg_id="m0")
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("s", "m0", 2)])  # the first delivery never reached the handler

        bus = self.bus(0, FaultConfig(lose_delivery=1.0, max_redeliveries=5), max_attempts=3)
        rec = Recorder()
        bus.subscribe("t", rec.handler("s"))
        publish(bus, "t", msg_id="m0")
        bus.run_until_idle()
        self.assertEqual(rec.calls, [])
        self.assertEqual([(d.envelope.msg_id, d.attempts) for d in bus.dead_letters()], [("m0", 3)])

    def test_a_crashed_consumer_resumes_at_its_checkpoint_and_reads_again_what_it_had_not_settled(self):
        # S3 delivers m2, m1, m0 (descending logical time), the log order is m0, m1, m2: the consumer started at the end
        # of the log, so its checkpoint is the first message logged, whichever comes first
        outcomes = set()
        for seed in range(12):
            bus = self.bus(seed, FaultConfig(crash=1.0, max_crashes=1))
            sub = bus.subscribe("t", self.rec.handler("s"))
            first_id = sub.id
            for i in range(3):
                publish(bus, "t", i, key="k%d" % i, ts=10 - i, msg_id="m%d" % i)
            bus.run_until_idle()
            ids = self.rec.ids()
            outcomes.add(tuple(ids))
            crashes = [f for f in bus.fault_log() if f.kind is FaultKind.CRASH]
            self.assertEqual([f.msg_id for f in crashes], ["m2"])
            self.assertNotEqual(sub.id, first_id)  # a restarted consumer is a new registration
            self.assertEqual(ids[-3:], ["m2", "m1", "m0"])  # read again from the checkpoint, handled or not
            self.assertEqual(sorted(set(ids)), ["m0", "m1", "m2"])
            self.rec.calls.clear()
        # the crash came before or after the handler ran
        self.assertEqual(outcomes, {("m2", "m1", "m0"), ("m2", "m2", "m1", "m0")})

    def test_a_crash_on_the_fourth_of_five_messages_of_a_lane_replays_from_the_fourth(self):
        bus = self.bus(0)
        crashed = []

        def handler(delivery):
            self.rec.calls.append((delivery.envelope.msg_id, delivery.attempt))
            if delivery.envelope.msg_id == "m3" and not crashed:
                crashed.append(delivery.envelope.msg_id)
                bus.crash(sub)  # before the ack of m3

        sub = bus.subscribe("t", handler)
        for i in range(5):
            publish(bus, "t", i, key="k", ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [("m0", 1), ("m1", 1), ("m2", 1), ("m3", 1), ("m3", 1), ("m4", 1)])
        self.assertEqual([f.kind for f in bus.fault_log()], [FaultKind.CRASH])

    def test_the_redelivery_budget_survives_a_crash(self):
        # S3 delivers m1 (ts 1) before m0 (ts 2): after m1 the checkpoint is still the start of the log, so the crash
        # during m0 reads m1 again; m1 already had its injected redelivery and gets no second one
        bus = self.bus(0, FaultConfig(ack_lost=1.0, max_redeliveries=1))
        crashed = []

        def handler(delivery):
            self.rec.calls.append((delivery.envelope.msg_id, delivery.attempt))
            if delivery.envelope.msg_id == "m0" and not crashed:
                crashed.append(1)
                bus.crash(sub)

        sub = bus.subscribe("t", handler)
        publish(bus, "t", key="a", ts=2, msg_id="m0")
        publish(bus, "t", key="b", ts=1, msg_id="m1")
        bus.run_until_idle()
        self.assertEqual(
            self.rec.calls, [("m1", 1), ("m1", 2), ("m0", 1), ("m1", 1), ("m0", 1), ("m0", 2)]
        )

    def test_a_crash_whose_replay_does_not_fit_is_skipped_and_the_subscription_lives(self):
        skipped = 0
        for seed in range(12):
            bus = self.bus(seed, FaultConfig(crash=0.5, max_crashes=2), queue_capacity=4)
            rec = Recorder()
            bus.subscribe("t", rec.handler("s"))
            for i in range(4):  # S3 delivers m3 first: the checkpoint stays at the start of the log until m0
                publish(bus, "t", i, key="k%d" % i, ts=9 - i, msg_id="m%d" % i)
            late = False
            for _ in range(100):
                if not late:
                    try:
                        publish(bus, "t", 4, key="k4", ts=1, msg_id="m4")  # the log is now longer than the capacity
                        late = True
                    except QueueFull:
                        pass
                if not bus.step() and late:
                    break
            self.assertEqual(sorted(set(rec.ids())), ["m0", "m1", "m2", "m3", "m4"], "seed %d" % seed)
            skipped += any(f.kind is FaultKind.CRASH_SKIPPED for f in bus.fault_log())
        self.assertGreater(skipped, 0)

        bus = self.bus(0, queue_capacity=3)
        sub = bus.subscribe("t", self.rec.handler("s"))
        for i in range(3):
            publish(bus, "t", i, key="k%d" % i, ts=9 - i, msg_id="m%d" % i)
        bus.step()  # m2: m0 and m1 are unsettled, the checkpoint is the start of the log
        publish(bus, "t", 3, key="k3", ts=1, msg_id="m3")
        with self.assertRaises(QueueFull):
            bus.crash(sub)  # the replay of four messages does not fit in three
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m2", "m3", "m1", "m0"])  # the old subscription went on

    def test_a_consumer_that_starts_at_a_position_crashes_back_to_it(self):
        bus = self.bus(1, FaultConfig(crash=1.0, max_crashes=1))
        receipt = publish(bus, "t", msg_id="m0")
        publish(bus, "t", msg_id="m1")
        bus.subscribe("t", self.rec.handler("s"), start=receipt.position)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids()[-2:], ["m0", "m1"])
        self.assertEqual(sorted(set(self.rec.ids())), ["m0", "m1"])

    def test_patterns_and_groups_do_not_crash(self):
        bus = self.bus(0, FaultConfig(crash=1.0))
        bus.subscribe("t.*", self.rec.handler("pattern"))
        bus.subscribe("t.a", self.rec.handler("g1"), group="g")
        bus.subscribe("t.a", self.rec.handler("g2"), group="g")
        for i in range(4):
            publish(bus, "t.a", i, msg_id="a%d" % i)
            publish(bus, "t.b", i, msg_id="b%d" % i)
        bus.run_until_idle()
        self.assertEqual([f for f in bus.fault_log() if f.kind is FaultKind.CRASH], [])
        self.assertEqual(len(self.rec.calls), 12)
        with self.assertRaises(ValueError):
            bus.crash(bus.subscribe("t.*", self.rec.handler("pattern")))

    def test_a_muted_participant_never_answers_and_comes_back_with_what_it_missed(self):
        bus = self.bus(0)
        answered = []

        def answer(delivery):
            answered.append(delivery.envelope.msg_id)
            reply(bus, delivery, delivery.envelope.payload * 2, type="t", logical_ts=1, producer_id="svc")

        service = bus.subscribe("svc", answer)
        watcher = Recorder()
        bus.subscribe("svc", watcher.handler("watcher"))
        bus.mute(service)
        pending = request(bus, "svc", 21, type="t", logical_ts=0, producer_id="client", msg_id="q1")
        bus.run_until_idle()
        self.assertFalse(pending.done)
        pending.send()
        bus.run_until_idle()
        self.assertFalse(pending.done)
        self.assertEqual(answered, [])
        self.assertEqual(watcher.ids(), ["q1", "q1"])  # the others are not affected
        self.assertEqual([f.kind for f in bus.fault_log()], [FaultKind.MUTED])
        bus.unmute(service)  # restarts at the checkpoint: nothing was acknowledged, so both requests come
        bus.run_until_idle()
        self.assertEqual(answered, ["q1", "q1"])
        self.assertTrue(pending.done)
        self.assertEqual(pending.reply.payload, 42)
        self.assertEqual([f.kind for f in bus.fault_log()], [FaultKind.MUTED, FaultKind.UNMUTED])

    def test_a_participant_can_fall_silent_by_itself(self):
        bus = self.bus(0, FaultConfig(mute=1.0))
        service = bus.subscribe("t", self.rec.handler("s"))
        for i in range(3):
            publish(bus, "t", i, key="k%d" % i, ts=i, msg_id="m%d" % i)
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])
        self.assertEqual([f.kind for f in bus.fault_log()], [FaultKind.MUTED])  # one delivery in hand, then nothing
        self.assertEqual(bus.dead_letters(), ())
        bus.unmute(service)  # a hang that does not end: with mute=1 the first delivery hangs it again
        bus.run_until_idle()
        self.assertEqual([f.kind for f in bus.fault_log()], [FaultKind.MUTED, FaultKind.UNMUTED, FaultKind.MUTED])
        with self.assertRaises(ValueError):
            bus.mute(object())

    def test_a_participant_that_hangs_sometimes_reads_everything_once_it_is_back(self):
        hung = 0
        for seed in range(6):
            bus = self.bus(seed, FaultConfig(mute=0.4))
            rec = Recorder()
            service = bus.subscribe("t", rec.handler("s"))
            for i in range(8):
                publish(bus, "t", i, key="k%d" % i, ts=i, msg_id="m%d" % i)
            for _ in range(200):
                bus.run_until_idle()
                if len(rec.calls) == 8:
                    break
                bus.unmute(service)
            self.assertEqual(rec.ids(), ["m%d" % i for i in range(8)], "seed %d" % seed)  # none comes twice
            hung += FaultKind.MUTED in {f.kind for f in bus.fault_log()}
        self.assertGreater(hung, 0)

    def test_a_mute_on_a_pattern_or_a_group_is_permanent(self):
        bus = self.bus(0)
        pattern = bus.subscribe("t.*", self.rec.handler("pattern"))
        member = bus.subscribe("t.a", self.rec.handler("member"), group="g")
        bus.mute(pattern)
        bus.mute(member)
        publish(bus, "t.a")
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [])
        for subscription in (pattern, member):
            with self.assertRaises(ValueError):
                bus.unmute(subscription)

    def test_reordering_inside_a_lane_needs_the_contract_violating_flag_and_breaks_fifo(self):
        with self.assertRaises(ValueError):
            FaultConfig(reorder_within_lane=0.5)
        violated = 0
        for seed in range(10):
            bus = self.bus(seed, FaultConfig(delay=1.0, max_delay=4, reorder_within_lane=1.0, contract_violating=True))
            rec = Recorder()
            bus.subscribe("t", rec.handler("s"))
            for i in range(6):
                publish(bus, "t", i, key="k", ts=i, msg_id="m%d" % i)
            bus.run_until_idle()
            self.assertEqual(sorted(rec.ids()), ["m%d" % i for i in range(6)])
            violated += rec.ids() != ["m%d" % i for i in range(6)]
            self.assertIn(FaultKind.REORDERED, {f.kind for f in bus.fault_log()})
        self.assertGreaterEqual(violated, 8)

    # the dispatch loop

    def test_run_until_idle_drains_what_is_held_and_idle_means_nothing_is_pending_anywhere(self):
        bus = self.bus(1, FaultConfig(delay=1.0, duplicate_publish=1.0, max_delay=50))
        bus.subscribe("t", self.rec.handler("s"))
        for i in range(5):
            publish(bus, "t", i, key="k%d" % i)
        self.assertEqual(bus.in_flight(), 10)
        self.assertEqual(self.rec.calls, [])
        self.assertEqual(bus.run_until_idle(), 10)
        self.assertEqual(bus.in_flight(), 0)
        self.assertFalse(bus.step())

    def test_the_limit_of_run_until_idle_counts_what_is_held(self):
        bus = self.bus(1, FaultConfig(delay=1.0, max_delay=5))
        bus.subscribe("t", self.rec.handler("s"))
        for i in range(3):
            publish(bus, "t", i, key="k%d" % i)
        with self.assertRaises(NotIdle):
            bus.run_until_idle(2)
        self.assertEqual(bus.run_until_idle(), 1)
        publish(bus, "t")
        self.assertEqual(bus.run_until_idle(1), 1)

    def test_step_cannot_be_nested(self):
        bus = self.bus(0, FaultConfig(delay=1.0))
        errors = []

        def handler(delivery):
            for call in (bus.step, bus.run_until_idle):
                try:
                    call()
                except BusError as error:
                    errors.append(error)

        bus.subscribe("t", handler)
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(len(errors), 2)

    def test_closing_drops_what_is_held(self):
        bus = self.bus(0, FaultConfig(delay=1.0, duplicate_publish=1.0))
        bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t")
        bus.close()
        self.assertEqual(bus.in_flight(), 0)
        with self.assertRaises(BusClosed):
            bus.run_until_idle()
        with self.assertRaises(BusClosed):
            publish(bus, "t")

    def test_a_handler_that_fails_is_still_retried_and_dead_lettered_by_the_inner_bus(self):
        bus = self.bus(0, FaultConfig(ack_lost=1.0))

        def failing(delivery):
            self.rec.calls.append(delivery.attempt)
            raise RuntimeError("no")

        bus.subscribe("t", failing)
        publish(bus, "t")
        bus.run_until_idle()
        self.assertEqual(self.rec.calls, [1, 2, 3])
        self.assertEqual([d.attempts for d in bus.dead_letters()], [3])

    def test_the_fault_log_says_what_was_injected_where(self):
        bus = self.bus(0, FaultConfig(ack_lost=1.0, delay=1.0, max_delay=2))
        sub = bus.subscribe("t", self.rec.handler("s"))
        publish(bus, "t", key="k", msg_id="m0")
        publish(bus, "t", key="k", msg_id="m1")
        bus.run_until_idle()
        log = bus.fault_log()
        self.assertEqual([f.index for f in log], list(range(len(log))))
        first, second = log[0], log[1]
        self.assertEqual((first.kind, first.topic, first.key, first.msg_id), (FaultKind.DELAYED, "t", "k", "m0"))
        self.assertEqual(second.kind, FaultKind.DELAYED)  # delay=1.0: both are delayed, and the lane keeps their order
        redelivered = [f for f in log if f.kind is FaultKind.DUPLICATE_DELIVERY]
        self.assertEqual([(f.msg_id, f.subscription) for f in redelivered], [("m0", sub.id), ("m1", sub.id)])
        self.assertTrue(all(isinstance(f.tick, int) and f.detail for f in redelivered))

    def test_every_call_draws_the_same_numbers_whatever_the_configuration(self):
        def states(faults):
            bus = self.bus(5, faults)
            bus.subscribe("t", self.rec.handler("s"))
            for i in range(60):
                try:
                    publish(bus, "t", i, key="k%d" % (i % 5), ts=i, msg_id="m%d" % i)
                except PublishFailed:
                    pass
            published = bus._rng.getstate()
            for _ in range(10):
                bus.step()
            return published, bus._rng.getstate()

        base = states(FaultConfig())
        for name in (
            "publish_fail",
            "publish_ack_lost",
            "delay",
            "duplicate_publish",
            "lose_delivery",
            "ack_lost",
            "crash",
            "mute",
            "reorder_within_lane",
        ):
            with self.subTest(name):
                faults = FaultConfig(**{name: 0.5}, contract_violating=name == "reorder_within_lane")
                got = states(faults)
                self.assertEqual(got[0], base[0])  # after the publishes
                if name != "mute":  # a muted subscription stops being delivered to
                    self.assertEqual(got[1], base[1])  # after the same number of deliveries

    def test_a_held_message_the_inner_bus_refuses_is_dropped_logged_and_its_receipt_cannot_be_used(self):
        if self.implementation != "serializing":
            self.skipTest("needs a codec that refuses a payload")
        bus = self.bus(0, FaultConfig(delay=1.0, max_delay=2))
        bus.subscribe("t", self.rec.handler("s"))
        receipt = publish(bus, "t", object(), key="bad", msg_id="bad")  # the codec has not seen it yet
        publish(bus, "t", 1, key="good", msg_id="good")
        with self.assertRaises(TypeError):
            bus.run_until_idle()
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["good"])
        refused = [f for f in bus.fault_log() if f.kind is FaultKind.RELEASE_FAILED]
        self.assertEqual([f.msg_id for f in refused], ["bad"])
        with self.assertRaises(ValueError):  # not silently LATEST
            bus.subscribe("t", self.rec.handler("again"), start=receipt.position)

    def test_a_held_message_waits_while_the_inner_bus_is_full(self):
        bus = self.bus(0, FaultConfig(delay=1.0, max_delay=1), queue_capacity=2)
        bus.subscribe("t", self.rec.handler("s"))
        for i in range(5):
            publish(bus, "t", i, key="k%d" % i, ts=i, msg_id="m%d" % i)
        self.assertEqual(bus.in_flight(), 5)
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["m%d" % i for i in range(5)])
        self.assertEqual(bus.in_flight(), 0)
        self.assertEqual([f for f in bus.fault_log() if f.kind is FaultKind.RELEASE_FAILED], [])

    def test_bad_arguments_are_not_reported_as_an_injected_failure(self):
        bus = self.bus(0, FaultConfig(publish_fail=1.0))
        with self.assertRaises(TypeError):
            publish(bus, "t", ts=1.5)
        with self.assertRaises(ValueError):
            publish(bus, "a.*")
        with self.assertRaises(PublishFailed):
            publish(bus, "t")

    def test_arguments_of_the_wrapper(self):
        with self.assertRaises(TypeError):
            SeededFaultBus(make_memory(), 1.5)
        for kwargs in ({"delay": 1.5}, {"crash": -0.1}, {"max_delay": 0}, {"max_redeliveries": -1}, {"mute": True}):
            with self.assertRaises(ValueError):
                FaultConfig(**kwargs)
        self.assertEqual(SeededFaultBus(make_memory(), 3).fault_log(), ())

    def test_a_position_that_is_not_a_receipt_of_the_wrapper_passes_through(self):
        bus = self.bus(0, FaultConfig())
        publish(bus, "t", 1, msg_id="a")
        publish(bus, "t", 2, msg_id="b")
        bus.subscribe("t", self.rec.handler("s"), start=Position("t", 1))
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["b"])


class TransparentContract(BusContract):
    """The broker-neutral contract over the wrapper without faults: every test of the suite passes."""

    def test_delivery_trace_is_independent_of_hash_seed(self):
        expected = json.loads(json.dumps(scenario_trace(type(self).make_bus)))
        for hash_seed in HASH_SEEDS:
            self.assertEqual(
                run_subprocess("transparent", self.implementation, hash_seed=hash_seed),
                expected,
                "PYTHONHASHSEED=" + hash_seed,
            )


class MemoryFaultBusTests(FaultBusTests, unittest.TestCase):
    implementation = "memory"


class SerializingFaultBusTests(FaultBusTests, unittest.TestCase):
    implementation = "serializing"


class MemoryTransparentContract(TransparentContract, unittest.TestCase):
    implementation = "memory"
    make_bus = staticmethod(transparent("memory"))


class SerializingTransparentContract(TransparentContract, unittest.TestCase):
    implementation = "serializing"
    make_bus = staticmethod(transparent("serializing"))


if __name__ == "__main__":
    if sys.argv[1] == "transparent":
        json.dump(scenario_trace(transparent(sys.argv[2])), sys.stdout)
    else:
        factory = lambda **options: faulty(sys.argv[1], SCENARIO_SEED, CHAOS, **options)  # noqa: E731
        json.dump(fault_scenario(factory), sys.stdout)
