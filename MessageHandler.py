
import gzip


class Tokenizer:
    def __init__(self, filename):
        self.filename = filename
        self.buffer = None
        self.buffer_size = 1024 * 4

    # to use with "with" statement
    def __enter__(self):
        self.f = gzip.open(self.filename, "rb")
        self.buffer = self.f.read(self.buffer_size)
        self.idx = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.close()

    def get_message(self):
        if self.idx >= len(self.buffer) - 1:
            self.buffer = self.buffer[self.idx: self.idx + 1] + self.f.read(self.buffer_size)
            if len(self.buffer) == 0:
                return b''

        if self.buffer[self.idx] != 0:
            raise RuntimeError("Unrecognized format")

        size = self.buffer[self.idx + 1]
        self.idx += 2 + size
        return self.buffer[self.idx - size: self.idx]
