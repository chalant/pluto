def write_perf(file_path, packet):
    with open(file_path, 'ab') as f:
        f.write(packet + b'END')

def read_perf(file_path):
    with open(file_path, 'rb') as f:
        packets = []
        while True:
            buffer = f.read(1024 * 64)
            if not buffer:
                if packets:
                    last = packets.pop()
                    if last:
                        yield last
                break
            else:
                packets.extend(buffer.split(b'END'))
                last_packet = packets.pop()
                for packet in packets:
                    if packet:
                        yield packet
                packets.clear()
                packets.append(last_packet)
