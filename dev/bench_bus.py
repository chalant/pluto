"""Dispatch cost of the in-memory bus: messages per second, with and without serialisation.

    python -m dev.bench_bus [MESSAGES]        (from the repository root; default 1,000,000)

Indicative only (one run, whatever else the machine is doing). "all at once" publishes every message and then
drains; "in batches" publishes 1,000 and drains, repeatedly, the pattern of a tick loop. A message counts once
however many subscribers receive it (with 4 subscribers each message is 4 deliveries). Keys are spread round-robin
over the keys (lanes) of the topic, with one ascending logical time per message.
"""
import sys
import time

from google.protobuf.timestamp_pb2 import Timestamp

from pluto.bus import InMemoryBus, ProtobufCodec, SerializingBus

BATCH = 1_000


def run(make_bus, count, payload, type, batch, keys, subscribers):
    bus = make_bus(queue_capacity=max(count, BATCH))
    received = 0

    def handler(delivery):
        nonlocal received
        received += 1

    for _ in range(subscribers):
        bus.subscribe("bench.topic", handler)
    key_names = [f"k{i}" for i in range(keys)]
    publish_time = drain_time = 0.0
    sent = 0
    while sent < count:
        n = min(batch, count - sent)
        t0 = time.perf_counter()
        for i in range(sent, sent + n):
            bus.publish("bench.topic", payload, type=type, logical_ts=i, producer_id="bench", key=key_names[i % keys])
        t1 = time.perf_counter()
        bus.run_until_idle()
        t2 = time.perf_counter()
        publish_time += t1 - t0
        drain_time += t2 - t1
        sent += n
    assert received == count * subscribers
    return publish_time, drain_time


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    codec = ProtobufCodec({"timestamp": Timestamp})

    def serializing(**options):
        return SerializingBus(InMemoryBus(**options), codec)

    timestamp = Timestamp(seconds=1, nanos=2)
    # (name, bus factory, payload, type, keys, subscribers)
    variants = (
        ("InMemoryBus 1 key, 1 subscriber", InMemoryBus, "plain", "t", 1, 1),
        ("InMemoryBus 16 keys, 1 subscriber", InMemoryBus, "plain", "t", 16, 1),
        ("InMemoryBus 1 key, 4 subscribers", InMemoryBus, "plain", "t", 1, 4),
        ("InMemoryBus 16 keys, 4 subscribers", InMemoryBus, "plain", "t", 16, 4),
        ("Serializing protobuf, 1 key, 1 sub", serializing, timestamp, "timestamp", 1, 1),
        ("Serializing JSON dict, 1 key, 1 sub", serializing, {"qty": 3, "id": "s1:7"}, "t", 1, 1),
    )
    for name, make_bus, payload, type, keys, subscribers in variants:
        for label, batch in (("all at once", count), ("in batches", BATCH)):
            publish_time, drain_time = run(make_bus, count, payload, type, batch, keys, subscribers)
            total = publish_time + drain_time
            print(
                f"{name:38s} {label:12s} {count / total:10,.0f} msg/s"
                f"  (publish {count / publish_time:10,.0f}/s, dispatch {count / drain_time:10,.0f}/s)",
                flush=True,
            )


if __name__ == "__main__":
    main()
