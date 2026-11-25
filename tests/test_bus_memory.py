"""What holds for the in-memory bus (bare and under the serialising wrapper) but is not part of the shared contract.

The contract (tests/test_bus_contract.py) must stay satisfiable by a broker. These tests cover the bounded queues
with their overflow policy (S12), log positions, and the group assignment of the in-memory bus.
"""
import unittest

from pluto.bus import BusError, Position, QueueFull, Start, request
from tests.test_bus_contract import Recorder, make_memory, make_serializing, publish


class InMemoryFamily:
    make_bus = None

    def setUp(self):
        self.rec = Recorder()

    def bus(self, **options):
        bus = type(self).make_bus(**options)
        self.addCleanup(bus.close)
        return bus

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

    def test_a_refused_publish_does_not_use_up_a_seq_or_reach_a_log(self):
        bus = self.bus(queue_capacity=2)
        bus.subscribe("a.*", self.rec.handler("s"))
        self.assertEqual(publish(bus, "a.x").msg_id, "p:1")
        self.assertEqual(publish(bus, "a.x").msg_id, "p:2")
        with self.assertRaises(QueueFull):
            publish(bus, "a.y")  # a new topic, refused
        bus.run_until_idle()
        seen = Recorder()
        bus.subscribe("a.*", seen.handler("replay"), start=Start.EARLIEST)  # a refused message in a log would
        bus.run_until_idle()  # make this replay exceed the capacity of 2
        self.assertEqual(seen.ids(), ["p:1", "p:2"])
        self.assertEqual(publish(bus, "a.x").msg_id, "p:3")  # not p:4

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
        """A QueueFull inside a handler is a handler failure: its retries run before the receiver drains, so
        unless the handler handles it the message is dead-lettered after max_attempts."""
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
        self.assertEqual(letter.error_type, "QueueFull")

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

    def test_a_request_that_overflows_leaves_no_subscription(self):
        bus = self.bus(queue_capacity=1)
        bus.subscribe("svc", lambda d: None)
        publish(bus, "svc")
        with self.assertRaises(QueueFull):
            request(bus, "svc", 1, type="t", logical_ts=0, producer_id="strat", msg_id="strat:1")
        bus.run_until_idle()
        publish(bus, "reply.strat")
        self.assertEqual(bus.run_until_idle(), 0)

    def test_capacity_and_attempts_must_be_positive_ints(self):
        for options in (dict(queue_capacity=0), dict(max_attempts=0), dict(queue_capacity=1.5), dict(max_attempts=True)):
            with self.assertRaises(ValueError):
                type(self).make_bus(**options)

    # positions

    def test_a_position_must_belong_to_the_topic_and_the_log(self):
        bus = self.bus()
        receipts = [publish(bus, "t", i) for i in range(2)]
        position = receipts[0].position
        for bad_start in (position, Position("other", 0)):
            with self.assertRaises(ValueError):
                bus.subscribe("t.*", lambda d: None, start=bad_start)
        for token in (3, -1, 1.5, True, None, "1"):
            with self.assertRaises(ValueError):
                bus.subscribe("t", lambda d: None, start=Position("t", token))
        with self.assertRaises(ValueError):
            bus.subscribe("other", lambda d: None, start=position)

    def test_a_position_in_a_topic_without_a_log_is_only_valid_at_zero(self):
        bus = self.bus()
        bus.subscribe("t", self.rec.handler("s"), start=Position("t", 0))
        with self.assertRaises(ValueError):
            bus.subscribe("t", lambda d: None, start=Position("t", 1))
        publish(bus, "t", msg_id="first")
        bus.run_until_idle()
        self.assertEqual(self.rec.ids(), ["first"])

    def test_position_of_needs_a_live_subscription(self):
        bus = self.bus()
        seen = []
        subscription = bus.subscribe("t", seen.append)
        publish(bus, "t")
        bus.run_until_idle()
        subscription.cancel()
        with self.assertRaises(BusError):
            bus.position_of(seen[0])

    # groups

    def test_a_group_assigns_a_key_to_the_same_member_whatever_the_order_of_creation(self):
        owners = []
        for keys in (["k%d" % i for i in range(8)], ["k%d" % i for i in reversed(range(8))]):
            bus = self.bus()
            bus.subscribe("t", self.rec.handler("g1"), group="g")
            bus.subscribe("t", self.rec.handler("g2"), group="g")
            self.rec.calls.clear()
            for i, key in enumerate(keys):
                publish(bus, "t", key=key, ts=i, msg_id=key)
            bus.run_until_idle()
            owners.append({msg_id: name for name, msg_id, _ in self.rec.calls})
        self.assertEqual(owners[0], owners[1])
        self.assertEqual(set(owners[0].values()), {"g1", "g2"})

    def test_a_group_started_from_earliest_keeps_the_replayed_keys_with_its_first_member(self):
        bus = self.bus()
        for i in range(6):
            publish(bus, "t", key="k%d" % i, ts=i)
        bus.subscribe("t", self.rec.handler("g1"), group="g", start=Start.EARLIEST)
        bus.subscribe("t", self.rec.handler("g2"), group="g")
        bus.run_until_idle()
        self.assertEqual(set(self.rec.names()), {"g1"})


class InMemoryBusTests(InMemoryFamily, unittest.TestCase):
    make_bus = staticmethod(make_memory)


class SerializingInMemoryBusTests(InMemoryFamily, unittest.TestCase):
    make_bus = staticmethod(make_serializing)


if __name__ == "__main__":
    unittest.main()
