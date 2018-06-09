
from collections import deque


class FormattedMessage:
    def __init__(self, raw=None):
        if raw is not None:
            self.type = raw[0]
            self.ref = int(raw[1])
            self.timestamp = int(raw[2])
            if self.type[0] == 'A':
                self.price = int(raw[4])
                self.shares = int(raw[5])
            elif self.type[0] == 'E' or self.type[0] == 'X':
                self.shares = int(raw[5])
            elif self.type[0] == 'U':
                self.new_ref = int(raw[3])
                self.price = int(raw[4])
                self.shares = int(raw[5])

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        if self.type[0] == 'A':
            return "{type: %s, ref: %d, timestamp: %d, price: %d, shares: %d}" % (self.type, self.ref, self.timestamp,
                                                                                  self.price, self.shares)
        elif self.type[0] == 'E':
            return "{type: %s, ref: %d, timestamp: %d, shares: %d}" % (self.type, self.ref, self.timestamp, self.shares)
        return "{type: %s, ref: %d, timestamp: %d}" % (self.type, self.ref, self.timestamp)


class Order:
    def __init__(self, ref, price, shares, real=True):
        self.ref = ref
        self.price = price
        self.shares = shares
        self.valid = True
        self.real = real  # the order is user generated or real data

    def __repr__(self):
        return "{ref: %s, price: %d, shares: %d, valid: %d, real: %d}" % (self.ref, self.price, self.shares, self.valid,
                                                                          self.real)

    def __str__(self):
        return self.__repr__()


class ExecutionInfo:
    def __init__(self, ref, price, shares):
        self.ref = ref
        self.price = price
        self.shares = shares

    def __repr__(self):
        return "{ref: %s, price: %d, shares: %d}" % (self.ref, self.price, self.shares)

    def __str__(self):
        return self.__repr__()


class Level:
    def __init__(self, price, order: Order):
        self.price = price
        self.queue = deque([order])

    def append(self, order: Order):
        self.queue.append(order)

    def popleft(self):
        self.queue.popleft()

    def first(self):
        return self.queue[0]

    def is_empty(self):
        return len(self.queue) == 0

    def __iter__(self):
        return self.queue.__iter__()


ask_comparators = [lambda x: x, lambda x, y: x > y]
bid_comparators = [lambda x: -x, lambda x, y: x < y]