import io

from protos import data_pb2

_DEFAULT_SIZE = 1024 * 64 #64kb

def chunk_bytes(bytes_, chunk_size=_DEFAULT_SIZE):
    with io.BytesIO(bytes_) as f:
        while True:
            buffer = f.read(chunk_size)
            if buffer == b'':
                break
            else:
                yield data_pb2.Chunk(data=buffer)