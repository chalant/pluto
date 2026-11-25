"""SerializingBus: what only a serialising wrapper does (the shared contract is tests/test_bus_contract.py)."""
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from pluto.bus import InMemoryBus, JsonCodec, ProtobufCodec, SerializingBus, Start


def publish(bus, topic, payload, type="t", **fields):
    return bus.publish(topic, payload, type=type, logical_ts=0, producer_id="p", **fields)


class SerializingBusTests(unittest.TestCase):
    def setUp(self):
        self.inner = InMemoryBus()
        self.bus = SerializingBus(self.inner, ProtobufCodec({"timestamp": Timestamp}))
        self.addCleanup(self.bus.close)

    def test_the_inner_bus_carries_bytes(self):
        raw = []
        self.inner.subscribe("t", raw.append)
        publish(self.bus, "t", {"a": [1, 2]})
        publish(self.bus, "t", Timestamp(seconds=3), type="timestamp")
        self.bus.run_until_idle()
        self.assertEqual(raw[0].envelope.payload, b'{"a":[1,2]}')
        self.assertEqual(raw[1].envelope.payload, Timestamp(seconds=3).SerializeToString())

    def test_every_subscription_gets_its_own_copy(self):
        seen = []

        def mutating(delivery):
            delivery.envelope.payload["a"].append(99)
            seen.append(delivery.envelope.payload)

        self.bus.subscribe("t", mutating)
        self.bus.subscribe("t", lambda d: seen.append(d.envelope.payload), rank=1)
        payload = {"a": [1]}
        publish(self.bus, "t", payload)
        self.bus.run_until_idle()
        self.assertEqual(seen, [{"a": [1, 99]}, {"a": [1]}])
        self.assertEqual(payload, {"a": [1]})
        replayed = []
        self.bus.subscribe("t", lambda d: replayed.append(d.envelope.payload), start=Start.EARLIEST)
        self.bus.run_until_idle()
        self.assertEqual(replayed, [{"a": [1]}])

    def test_protobuf_messages_come_back_as_new_equal_objects(self):
        seen = []
        self.bus.subscribe("t", lambda d: seen.append(d.envelope.payload))
        message = Timestamp(seconds=1_700_000_000, nanos=7)
        publish(self.bus, "t", message, type="timestamp")
        self.bus.run_until_idle()
        self.assertEqual(seen, [message])
        self.assertIsNot(seen[0], message)

    def test_json_fallback_turns_tuples_into_lists(self):
        seen = []
        self.bus.subscribe("t", lambda d: seen.append(d.envelope.payload))
        publish(self.bus, "t", {"pair": (1, 2), "none": None})
        self.bus.run_until_idle()
        self.assertEqual(seen, [{"pair": [1, 2], "none": None}])

    def test_a_payload_that_cannot_be_encoded_is_refused_to_the_publisher(self):
        with self.assertRaises(TypeError):
            publish(self.bus, "t", object())
        with self.assertRaises(TypeError):
            publish(self.bus, "t", {"not": "a timestamp"}, type="timestamp")
        raw = []
        self.inner.subscribe("t", raw.append, start=Start.EARLIEST)
        self.inner.run_until_idle()
        self.assertEqual(raw, [])  # nothing was logged

    def test_the_codec_is_pluggable(self):
        class Upper:
            def encode(self, payload, type):
                return payload.upper().encode()

            def decode(self, data, type):
                return data.decode().lower()

        bus = SerializingBus(self.inner, Upper())
        seen = []
        bus.subscribe("t", lambda d: seen.append(d.envelope.payload))
        raw = []
        self.inner.subscribe("t", raw.append)
        publish(bus, "t", "abc")
        bus.run_until_idle()
        self.assertEqual((seen, raw[0].envelope.payload), (["abc"], b"ABC"))

    def test_the_protobuf_codec_takes_a_fallback(self):
        codec = ProtobufCodec({"timestamp": Timestamp}, fallback=JsonCodec())
        data = codec.encode([1, "x"], "other")
        self.assertEqual(codec.decode(data, "other"), [1, "x"])


if __name__ == "__main__":
    unittest.main()
