
import gzip
from utils import Message


class Tokenizer:
    def __init__(self, filename):
        self.filename = filename
        self.buffer = None
        self.buffer_size = 1024 * 4
        self.idx = 0

    # to use with "with" statement
    def __enter__(self):
        self.f = gzip.open(self.filename, "rb")
        self.buffer = self.f.read(self.buffer_size)
        self.idx = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.close()

    def get_message(self):
        if self.idx >= len(self.buffer) - 100:
            self.buffer = self.buffer[self.idx:] + self.f.read(self.buffer_size)
            self.idx = 0
            if len(self.buffer) == 0:
                return b''

        if self.buffer[self.idx] != 0:
            raise RuntimeError("Unrecognized format")

        size = self.buffer[self.idx + 1]
        self.idx += 2 + size
        return self.buffer[self.idx - size: self.idx]


def parse_message(msg):
    if msg[0] == 83:
        return Message.SystemEvent(msg)
    elif msg[0] == 86:
        return Message.MwcbDeclineLevel(msg)
    elif msg[0] == 82:
        return Message.StockDirectory(msg)
    elif msg[0] == 72:
        return Message.TradingAction(msg)
    elif msg[0] == 89:
        return Message.RegShoRestriction(msg)
    elif msg[0] == 76:
        return Message.MarketParticipant(msg)
    elif msg[0] == 65:
        return Message.OrderAdd(msg)
    elif msg[0] == 68:
        return Message.OrderDelete(msg)
    elif msg[0] == 88:
        return Message.OrderCancel(msg)
    elif msg[0] == 85:
        return Message.OrderReplace(msg)
    elif msg[0] == 69:
        return Message.OrderExecuted(msg)
    elif msg[0] == 80:
        return Message.NonCrossTrade(msg)
    elif msg[0] == 78:
        return Message.RetailPriceImprovement(msg)
    elif msg[0] == 7011:
        return Message.BrokenTrade(msg)
    elif msg[0] == 70:
        return Message.OrderAddMpid(msg)
    elif msg[0] == 67:
        return Message.OrderExecutedWithPrice(msg)
    else:
        return None