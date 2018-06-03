"""
OrderBook implementation using SortedList
"""
from sortedcontainers import SortedList
from collections import deque, OrderedDict


class FormattedMessage:
    def __init__(self, raw=None):
        if raw is not None:
            self.type = raw[0]
            self.ref = int(raw[2])
            self.timestamp = int(raw[3])
            if self.type == 'A':
                self.type += 'B' if raw[4] == '1' else 'A'
                self.price = int(raw[5])
                self.shares = int(raw[6])
            elif self.type == 'E' or self.type == 'X':
                self.shares = int(raw[6])
            elif self.type == 'C':
                self.price = int(raw[5])
                self.shares = int(raw[6])
            elif self.type == 'U':
                self.new_ref = int(raw[4])
                self.price = int(raw[5])
                self.shares = int(raw[6])

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "type: %s\nref: %d\ntimestamp: %d" % (self.type, self.ref, self.timestamp)


class Order:
    def __init__(self, ref, price, shares, real=True):
        self.ref = ref
        self.price = price
        self.shares = shares
        self.valid = True
        self.real = real  # the order is user generated or real data


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


class Book:
    """
    For order modification functions, we assume that the ref has been checked in the whole book level
    """
    def __init__(self, comparators=ask_comparators):
        self.levels = SortedList(key=comparators[0])
        self.default_quote = 100000000 if comparators[0](1) == 1 else 0  # quote before book is initialized
        self.later_than = comparators[1]
        self.pool = {}  # record orders for update or deletion
        self.level_pool = {}  # store leveals
        self.ref_pool = {}
        self.volumes = OrderedDict()

    def __contains__(self, item):
        return item in self.pool or item in self.ref_pool

    def get_front_order(self) -> Order:
        return self.level_pool[self.levels[0]][0]

    def get_front_real_order(self):
        """
        the foremost real (not generated) order
        every order related function should call update_book at the end. We shouldn't need to do it again here
        """
        for price in self.levels:
            level = self.level_pool[price]
            for order in level:
                if order.real:
                    return order
        return None

    def get_quote(self):
        try:
            return self.levels[0]
        except IndexError:
            return self.default_quote

    def get_quote_volume(self):
        return self.volumes[self.levels[0]]

    def update_volume(self, price, shares):
        if price in self.volumes:
            self.volumes[price] += shares
        else:
            self.volumes[price] = shares
        if len(self.volumes) > 20000:
            raise RuntimeError("Too many volume levels")

    def update_book(self):
        while len(self.levels) > 0:
            level = self.level_pool[self.levels[0]]  # get first level
            while len(level) > 0 and not level[0].valid:
                level.popleft()
            if len(level) > 0:
                break
            else:
                del self.level_pool[self.levels[0]]
                self.levels.pop(0)

    def remove(self, ref):
        tmp = self.pool.pop(ref)
        tmp.valid = False

    def add_order(self, ref, price: float, shares, real=True):
        order = Order(ref, price, shares, real)
        self.pool[ref] = order
        if price not in self.level_pool:
            self.level_pool[price] = deque([order])
            self.levels.add(price)
        else:
            self.level_pool[price].append(order)
        self.update_volume(price, shares)

    def execute_order(self, ref, shares, ask=True):
        # we assume that when there is algo market order, there will be NO algo limit order on the same book
        # we also assume we will never run out of limit orders
        self.update_book()
        # if algo market order or the target order is not on the first level or execution is on the real front order
        # then we should walk the book starting from the front order
        if ref < 0 \
                or self.later_than(self.pool[ref].price, self.get_quote()) \
                or ref == self.get_front_real_order().ref:
            executed = []
            prev_shares = shares
            while shares > 0:
                tmp = self.get_front_order()
                if tmp.shares <= shares:
                    self.remove(tmp.ref)
                    shares -= tmp.shares
                    self.update_volume(tmp.price, -tmp.shares)
                    if tmp.ref != ref:  # the order would not be execute were it not for algo orders, should save ref no
                        self.ref_pool[tmp.ref] = None  # use dict instead of set because it's faster
                else:
                    tmp.shares -= shares
                    self.update_volume(tmp.price, -shares)
                    shares = 0
                self.update_book()
                if ref < 0:  # report algo order execution
                    executed.append([tmp.price, prev_shares - shares])
                elif not tmp.real:
                    executed.append([tmp.price, prev_shares - shares])
                prev_shares = shares
            self.ref_pool.pop(ref, None)  # it's possible that ref is a ref_pool order, so remove it after it's used
            return ask ^ (ref > 0), executed
        else:
            tmp = self.pool[ref]
            tmp.shares -= shares
            if tmp.shares < 0:
                raise RuntimeError("Special execution handling failed")
            if tmp.shares == 0:
                self.remove(ref)
            self.update_book()
            self.update_volume(tmp.price, -shares)
            return None, []

    def execute_order_with_price(self, ref, price, shares):
        self.execute_order(ref, shares)

    def cancel_order(self, ref, shares):
        tmp = self.pool[ref]
        if not tmp.valid or tmp.shares < shares:
            raise RuntimeError("Order cancellation error - order specs mismatch")
        tmp.shares -= shares
        self.update_volume(tmp.price, -shares)

    def delete_order(self, ref):
        tmp = self.pool[ref]
        self.update_volume(tmp.price, -tmp.shares)
        self.remove(ref)
        self.update_book()

    def replace_order(self, ref, new_ref, price, shares):
        self.delete_order(ref)
        self.add_order(new_ref, price, shares)


class OrderBook:
    def __init__(self):
        self.ask_book = Book(ask_comparators)
        self.bid_book = Book(bid_comparators)

    def get_spread(self):
        return self.ask_book.get_quote() - self.bid_book.get_quote()

    def get_mid_price(self):
        return (self.ask_book.get_quote() + self.bid_book.get_quote()) / 2

    def process_message(self, msg: FormattedMessage):
        price, shares = None, None
        if msg.type == 'AA':  # add Ask
            self.add_ask(msg.ref, msg.price, msg.shares)
        if msg.type == 'AA2':  # add algo generated ask
            self.add_ask(msg.ref, msg.price, msg.shares, False)
        elif msg.type == 'AB':  # ask bid
            self.add_bid(msg.ref, msg.price, msg.shares)
        elif msg.type == 'AB2':  # ask algo generated bid
            self.add_bid(msg.ref, msg.price, msg.shares, False)
        elif msg.type == 'E':
            return self.execute_order(msg.ref, msg.shares)
        elif msg.type == 'C':
            # price, shares = self.execute_order_with_price(msg.ref, msg.price, msg.shares)
            return self.execute_order(msg.ref, msg.shares)
        elif msg.type == 'X':  # cancel
            self.cancel_order(msg.ref, msg.shares)
        elif msg.type == 'D':  # delete
            self.delete_order(msg.ref)
        elif msg.type == 'U':  # update
            self.replace_order(msg.ref, msg.new_ref, msg.price, msg.shares)
        return price, shares

    def add_bid(self, ref, price, shares, real=True):
        if price < self.ask_book.get_quote():
            self.bid_book.add_order(ref, price, shares, real)
        else:
            self.ask_book.execute_order(ref, shares)  # cross the book, this can happen when algo msg is delayed

    def add_ask(self, ref, price, shares, real=True):
        if price > self.bid_book.get_quote():
            self.ask_book.add_order(ref, price, shares, real)
        else:
            self.bid_book.execute_order(ref, shares)  # cross the book, this can happen when algo msg is delayed

    def execute_order(self, ref, shares):
        if ref in self.ask_book or ref == -1:
            return self.ask_book.execute_order(ref, shares)
        elif ref in self.bid_book or ref == -2:
            return self.bid_book.execute_order(ref, shares, False)
        else:
            raise RuntimeError("Execution error - ref not exists")

    def execute_order_with_price(self, ref, price, shares):
        if ref in self.ask_book:
            self.ask_book.execute_order_with_price(ref, price, shares)
        elif ref in self.bid_book:
            self.bid_book.execute_order_with_price(ref, price, shares)
        else:
            raise RuntimeError("Execution error - ref not exists")

    def cancel_order(self, ref: int, shares: int):
        if ref in self.ask_book:
            self.ask_book.cancel_order(ref, shares)
        elif ref in self.bid_book:
            self.bid_book.cancel_order(ref, shares)
        else:
            raise RuntimeError("Cancellation error - ref not exists")

    def delete_order(self, ref: int):
        if ref in self.ask_book:
            self.ask_book.delete_order(ref)
        elif ref in self.bid_book:
            self.bid_book.delete_order(ref)
        else:
            raise RuntimeError("Deletion error - ref not exists")

    def replace_order(self, ref: int, new_ref:int, price: int, shares: int):
        if ref in self.ask_book:
            self.ask_book.replace_order(ref, new_ref, price, shares)
        elif ref in self.bid_book:
            self.bid_book.replace_order(ref, new_ref, price, shares)
        else:
            raise RuntimeError("Replacement error - ref not exists")
