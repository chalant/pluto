def write_perf(file_path, packet):
    with open(file_path, 'ab') as f:
        f.write(packet + b'END')


def read_perf(file_path):
    with open(file_path, 'rb') as f:
        packets = []
        size = 1024 * 64 #64 KB
        while True:
            buffer = f.read(size)
            if not buffer:
                if packets:
                    last = packets.pop()
                    if last:
                        yield last
                break
            else:
                try:
                    # combine the last packet with the current buffer
                    buffer = packets.pop() + buffer
                except IndexError:
                    pass
                packets.extend(buffer.split(b'END'))
                last_packet = packets.pop()
                for packet in packets:
                    if packet:
                        yield packet
                packets.clear()
                packets.append(last_packet)
