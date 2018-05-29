
import abc
import struct


class Message(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __str__(self):
        pass

    def _print(self, keys: list):
        res = ""
        for key in keys:
            if hasattr(self, key):
                res += key + ": " + str(self.__getattribute__(key)) + "\n"
            else:
                res += key + "\n"
        return res


class SystemEvent(Message):
    def __init__(self, message):
        self.tracking, self.timestamp, self.event = struct.unpack("!H6sc", message[3:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["tracking", "timestamp", "event"])


class MwcbDeclineLevel(Message):
    def __init__(self, msg):
        self.tracking, _, self.timestamp = struct.unpack("!HHI", msg[3: 11])
        self.timestamp |= _ << 16

    def __str__(self):
        return self._print(["tracking", "timestamp"])


class StockDirectory(Message):
    def __init__(self, msg):
        self.locate, self.tracking, _, self.timestamp, self.stock, self.exchange, self.status, self.lot = \
            struct.unpack("!HHHI8sccl", msg[1:25])
        self.timestamp |= _ << 16

    def __str__(self):
        return self._print(["timestamp", "locate", "tracking", "stock", "exchange", "status", "lot"])


class TradingAction(Message):
    def __init__(self, msg):
        self.locate, self.tracking, _, self.timestamp, self.stock, self.status, self.reserved, self.reason = \
            struct.unpack("!HHHI8scc4s", msg[1:])
        self.timestamp |= _ << 16

    def __str__(self):
        return self._print(["timestamp", "locate", "tracking", "stock", "status", "reason"])


class RegShoRestriction(Message):
    def __init__(self, msg):
        pass

    def __str__(self):
        return "Y"


class MarketParticipant(Message):
    def __init__(self, msg):
        self.locate, self.tracking, _, self.timestamp, self.mpid, self.stock, self.primary_maker, self.mode, self.state \
            = struct.unpack("!HHHI4s8sccc", msg[1:])
        self.timestamp |= _ << 16

    def __str__(self):
        return self._print(["timestamp", "locate", "tracking", "mpid", "stock", "mode", "state"])


class OrderAdd(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.buy_sell, self.shares, self.stock, self.price = \
            struct.unpack("!HH6sQcI8sI", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "stock", "price", "shares"])


class OrderAddMpid(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.buy_sell, self.shares, self.stock, self.price, \
        self.mpid = struct.unpack("!HH6sQcI8sI4s", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "ref", "stock", "price", "shares", "buy_sell"])


class OrderDelete(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref = struct.unpack("!HH6sQ", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "ref", "Delete"])


class OrderCancel(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.shares = struct.unpack("!HH6sQI", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return  self._print(["timestamp", "ref", "Cancel", "shares"])


class OrderReplace(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.new_ref, self.shares, self.price \
            = struct.unpack("!HH6sQQII", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "ref", "new_ref", "price", "shares"])


class OrderExecuted(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.shares, self.match \
            = struct.unpack("!HH6sQIQ", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "ref", "match", "shares"])


class OrderExecutedWithPrice(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.shares, self.match, self.printable, self.price \
            = struct.unpack("!HH6sQIQcI", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "ref", "match", "price", "shares"])


class NonCrossTrade(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.ref, self.type, self.shares, self.stock, self.price, \
        self.match = struct.unpack("!HH6sQcI8sIQ", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        # ref number will always be 0 for non-cross
        return self._print(["timestamp", "stock", "price", "shares", "type"])


class RetailPriceImprovement(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.stock, self.type = struct.unpack("!HH6s8sc", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "stock", "type"])


class BrokenTrade(Message):
    def __init__(self, msg):
        self.locate, self.tracking, self.timestamp, self.match = struct.unpack("!HH6sQ", msg[1:])
        self.timestamp = int.from_bytes(self.timestamp, "big")

    def __str__(self):
        return self._print(["timestamp", "match"])

