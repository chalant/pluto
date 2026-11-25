"""Dispatch cost of the in-memory bus: messages per second, one subscriber, with and without serialisation.

    python -m dev.bench_bus [MESSAGES]        (from the repository root; default 1,000,000)

Indicative only (one run, whatever else the machine is doing). "all at once" publishes every message and then
drains; "in batches" publishes 1,000 and drains, repeatedly, the pattern of a tick loop.
"""
import sys
import time

from google.protobuf.timestamp_pb2 import Timestamp

from pluto.bus import InMemoryBus, ProtobufCodec, SerializingBus

BATCH = 1_000


def run(make_bus, count, payload, type, batch):
    bus = make_bus(queue_capacity=max(count, BATCH))
    received = 0

    def handler(delivery):
        nonlocal received
        received += 1

    bus.subscribe("bench.topic", handler)
    publish_time = drain_time = 0.0
    sent = 0
    while sent < count:
        n = min(batch, count - sent)
        t0 = time.perf_counter()
        for i in range(sent, sent + n):
            bus.publish("bench.topic", payload, type=type, logical_ts=i, producer_id="bench", key="k")
        t1 = time.perf_counter()
        bus.run_until_idle()
        t2 = time.perf_counter()
        publish_time += t1 - t0
        drain_time += t2 - t1
        sent += n
    assert received == count
    return publish_time, drain_time


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    codec = ProtobufCodec({"timestamp": Timestamp})
    variants = (
        ("InMemoryBus", InMemoryBus, "plain", "t"),
        ("SerializingBus, protobuf Timestamp", lambda **kw: SerializingBus(InMemoryBus(**kw), codec), Timestamp(seconds=1, nanos=2), "timestamp"),
        ("SerializingBus, JSON dict", lambda **kw: SerializingBus(InMemoryBus(**kw), codec), {"qty": 3, "id": "s1:7"}, "t"),
    )
    for name, make_bus, payload, type in variants:
        for label, batch in (("all at once", count), ("in batches", BATCH)):
            publish_time, drain_time = run(make_bus, count, payload, type, batch)
            total = publish_time + drain_time
            print(
                f"{name:36s} {label:12s} {count / total:12,.0f} msg/s total"
                f"  (publish {count / publish_time:12,.0f}/s, dispatch {count / drain_time:12,.0f}/s)"
            )


if __name__ == "__main__":
    main()
