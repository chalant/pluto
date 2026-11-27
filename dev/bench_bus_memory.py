"""Memory held by a bus, per structure, after a whole run and after a long synthetic one.

    python -m dev.bench_bus_memory synthetic memory|nats [MESSAGES] [WINDOW]   (from the repository root)
    pytest dev/bench_bus_memory.py -s -q -W ignore                            (`buy_and_hold`, in memory and over NATS)

The size of each attribute of the bus (and of the inner bus of a wrapper) is the deep size of what it holds: containers,
envelopes, strings, bytes; functions, classes and modules are not followed (a handler's closure is not the bus). Objects shared
between attributes are counted once, for the attribute met first, so the order of the table is the order of the attributes.
Payloads are bytes (a `SerializingBus`), the process's resident memory is printed next to the sum. WINDOW is
`Retention(max_messages=WINDOW)` when the bus has it.

The synthetic run: 3 topics, 4 keys, 4 producers, 100-byte payloads, two subscriptions (one on a pattern, one on a topic that
stores `position_of` now and then), published in batches of 1,000 and drained, like a tick loop.
"""
import gc
import sys
import time
import types
from collections import deque

SKIP = (types.FunctionType, types.MethodType, types.BuiltinFunctionType, types.ModuleType, type, types.CellType)


def deep_size(obj, seen):
    """Bytes held by `obj`, not counting what `seen` already has (ids), following containers and instances."""
    stack = [obj]
    total = 0

    while stack:
        item = stack.pop()

        if id(item) in seen or isinstance(item, SKIP):
            continue

        seen.add(id(item))
        total += sys.getsizeof(item)

        if isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, (list, tuple, set, frozenset, deque)):
            stack.extend(item)
        elif hasattr(item, '__dict__'):
            stack.append(vars(item))

        for cls in type(item).__mro__:
            for name in getattr(cls, '__slots__', ()):
                if name in ('__dict__', '__weakref__') or not hasattr(item, name):
                    continue
                stack.append(getattr(item, name))

    return total


def structures(bus):
    """(name, deep size) for every attribute of the bus and of the buses it wraps, the first claimer keeping shared objects."""
    rows = []
    layers = []
    layer = bus

    while layer is not None:
        layers.append(layer)
        layer = getattr(layer, '_inner', None)

    # subscriptions point back at their bus, and the log of a LogBus is a connection: neither is the bus's own memory
    seen = {id(layer) for layer in layers}

    for layer in layers:
        for name, value in vars(layer).items():
            if name in ('_inner', '_log') or isinstance(value, SKIP):
                continue

            rows.append((type(layer).__name__ + '.' + name, deep_size(value, seen)))

    return rows


def rss():
    with open('/proc/self/status') as status:
        for line in status:
            if line.startswith('VmRSS'):
                return int(line.split()[1]) * 1024


def report(title, bus, messages, seconds=None):
    gc.collect()
    rows = [row for row in structures(bus) if row[1] > 256]
    rows.sort(key=lambda row: -row[1])
    total = sum(size for _, size in structures(bus))
    print('\nMEMORY {}: {:,} messages{}'.format(title, messages, '' if seconds is None else ', {:.1f} s'.format(seconds)))

    for name, size in rows:
        print('  {:<42s} {:>12,} B'.format(name, size))

    print('  {:<42s} {:>12,} B  ({:,.0f} B per message), process RSS {:,} B'.format(
        'sum', total, total / max(messages, 1), rss()))
    return total


def synthetic(make_bus, count, batch=1_000):
    bus = make_bus()
    seen = [0]
    checkpoints = []
    payload = 'x' * 100
    topics = ['t.0', 't.1', 't.2']

    def count_handler(delivery):
        seen[0] += 1

    def checkpoint(delivery):
        if delivery.envelope.seq % 1000 == 0:
            checkpoints.append(bus.position_of(delivery))
            del checkpoints[:-4]

    bus.subscribe('t.>', count_handler)
    bus.subscribe('t.0', checkpoint, rank=1)
    started = time.perf_counter()
    sent = 0

    while sent < count:
        for i in range(sent, min(sent + batch, count)):
            bus.publish(
                topics[i % 3], payload, type='t', logical_ts=i, producer_id='p%d' % (i % 4), key='k%d' % (i % 4))

        bus.run_until_idle()
        sent += batch

    assert seen[0] == count
    return bus, time.perf_counter() - started


def main():
    assert sys.argv[1] == 'synthetic', __doc__
    kind = sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 1_000_000
    window = int(sys.argv[4]) if len(sys.argv) > 4 else None
    options = {}

    if window is not None:
        from pluto.bus import Retention
        options['retention'] = Retention(max_messages=window)

    if kind == 'memory':
        from pluto.bus import InMemoryBus, JsonCodec, SerializingBus

        def make():
            return SerializingBus(InMemoryBus(**options), JsonCodec())
    else:
        from pluto.bus import JsonCodec, SerializingBus
        from pluto.bus.jetstream import JetStreamLog
        from pluto.bus.log import LogBus

        def make():
            return SerializingBus(LogBus(JetStreamLog(drop_on_close=True), **options), JsonCodec())

    before = rss()
    bus, seconds = synthetic(make, count)
    report('synthetic over {}{}'.format(kind, '' if window is None else ', window {:,}'.format(window)), bus, count, seconds)
    print('  RSS grew by {:,} B'.format(rss() - before))
    bus.close()


if __name__ == '__main__':
    main()
