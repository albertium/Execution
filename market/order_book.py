"""
OrderBook implementation using SortedList
"""
from sortedcontainers import SortedList
from collections import deque, OrderedDict


class FormattedMessage:
    def __init__(self, raw, real=True):
        self.type = raw[0]
        self.ref = int(raw[2])
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


class Order:
    def __init__(self, ref, price, shares):
        self.ref = ref
        self.price = price
        self.shares = shares
        self.valid = True
        self.real = True  # the order is user generated or real data


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
        self.later_than = comparators[1]
        self.pool = {}  # record orders for update or deletion
        self.level_pool = {}  # store leveals
        self.volumes = OrderedDict()

    def __contains__(self, item):
        return item in self.pool

    def get_front_order(self):
        return self.level_pool[self.levels[0]][0]

    def get_ref_price(self):
        # reference price
        return self.levels[0]

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

    def add_order(self, ref, price: float, shares):
        order = Order(ref, price, shares)
        self.pool[ref] = order
        if price not in self.level_pool:
            self.level_pool[price] = deque([order])
            self.levels.add(price)
        else:
            self.level_pool[price].append(order)
        self.update_volume(price, shares)

    def execute_order(self, ref, shares):
        self.update_book()
        tmp = self.pool[ref]
        if self.later_than(tmp.price, self.get_ref_price()):
            raise RuntimeError("Order execution error - execution not on the best level")
        if tmp.shares < shares:
            raise RuntimeError("Order execution error - executed more than available")
        elif tmp.shares > shares:
            tmp.shares -= shares
        else:
            self.remove(ref)
        self.update_book()
        self.update_volume(tmp.price, -shares)

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


class SimulationBook(Book):
    """
    allow algo generated order
    """
    def __init__(self, comparators):
        super(SimulationBook, self).__init__(comparators)
        self.ref_pool = {}

    def __contains__(self, item):
        return item in self.pool or item in self.ref_pool

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

    def execute_order(self, ref, shares):
        self.update_book()
        tmp = self.pool[ref]
        # if the target order is not on the first level or execution is on the real front order
        # then we should walk the book starting from the front order
        if self.later_than(tmp.price, self.get_ref_price()) or ref == self.get_front_real_order().ref:
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
            self.ref_pool.pop(ref, None)  # it's possible that ref is a ref_pool order, so remove it after it's used
        else:
            tmp.shares -= shares
            if tmp.shares == 0:
                self.remove(ref)
            self.update_book()
            self.update_volume(tmp.price, -shares)


class OrderBook:
    def __init__(self, book_type=Book):
        self.ask_book = book_type(ask_comparators)
        self.bid_book = book_type(bid_comparators)

    def get_spread(self):
        return self.ask_book.get_ref_price() - self.bid_book.get_ref_price()

    def get_mid_price(self):
        return (self.ask_book.get_ref_price() + self.bid_book.get_ref_price()) / 2

    def process_message(self, row):
        order_type = row[0]
        ref = int(row[2])
        if order_type == 'A':  # add order
            if row[4] == '1':  # row[4] is buy/sell indicator
                self.add_bid(ref, int(row[5]), int(row[6]))
            else:
                self.add_ask(ref, int(row[5]), int(row[6]))
        elif order_type == 'E':
            self.execute_order(ref, int(row[6]))
        elif order_type == 'C':
            self.execute_order_with_price(ref, int(row[5]), int(row[6]))
        elif order_type == 'X':  # cancel
            self.cancel_order(ref, int(row[6]))
        elif order_type == 'D':  # delete
            self.delete_order(ref)
        elif order_type == 'U':  # update
            self.replace_order(ref, int(row[4]), int(row[5]), int(row[6]))

    def add_bid(self, ref, price, shares):
        self.bid_book.add_order(ref, price, shares)

    def add_ask(self, ref, price, shares):
        self.ask_book.add_order(ref, price, shares)

    def execute_order(self, ref, shares):
        if ref in self.ask_book:
            self.ask_book.execute_order(ref, shares)
        elif ref in self.bid_book:
            self.bid_book.execute_order(ref, shares)
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
