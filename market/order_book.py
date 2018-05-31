"""
OrderBook implementation using SortedList
"""
from sortedcontainers import SortedList
from collections import deque


class Order:
    def __init__(self, ref, price, shares):
        self.ref = ref
        self.price = price
        self.shares = shares
        self.valid = True
        self.generated = True  # the order is user generated or from historical data


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


class Wrapper:
    def __init__(self, price):
        self.price = price


ask_comparators = [lambda x: x.price, lambda x, y: x > y]
bid_comparators = [lambda x: -x.price, lambda x, y: x < y]


class Book:
    """
    linear in the first nth element and heap afterward
    For order modification functions, we assume that the ref has been checked in the whole book level
    """
    def __init__(self, comparators=ask_comparators):
        self.levels = SortedList(key=comparators[0])
        self.later_than = comparators[1]
        self.pool = {}  # record orders for update or deletion
        self.ref_price = None  # best bid or offer

    def __contains__(self, item):
        return item in self.pool

    def update_book(self):
        while not self.levels[0].first().valid:
            self.levels[0].popleft()
            if self.levels[0].is_empty():
                self.levels.pop(0)
        self.ref_price = self.levels[0].price

    def add_order(self, ref, price: float, shares):
        order = Order(ref, price, shares)
        self.pool[ref] = order
        try:
            level = self.levels.index(Wrapper(price))
            level.append(order)
        except ValueError:
            self.levels.add(Level(price, order))

    def execute_order(self, ref, shares):
        self.update_book()
        tmp = self.pool[ref]
        if self.later_than(tmp.price, self.ref_price):
            raise RuntimeError("Order execution error - execution not on the best level")
        if tmp.shares < shares:
            raise RuntimeError("Order execution error - executed more than available")
        tmp.shares -= shares
        if tmp.shares == 0:
            tmp.valid = False
            self.pool.pop(ref, None)

    def execute_order_with_price(self, ref, price, shares):
        self.execute_order(ref, shares)

    def cancel_order(self, ref, shares):
        tmp = self.pool[ref]

        if not tmp.valid or tmp.shares < shares:
            raise RuntimeError("Order cancellation error - order specs mismatch")
        tmp.shares -= shares
        if tmp.shares == 0:
            print("cancel to 0\n")
            tmp.valid = False
            self.pool.pop(ref, None)

    def delete_order(self, ref):
        self.pool[ref].valid = False
        self.pool.pop(ref)

    def replace_order(self, ref, new_ref, price, shares):
        self.delete_order(ref)
        self.add_order(new_ref, price, shares)


class OrderBook:
    def __init__(self):
        self.ask_book = Book(ask_comparators)
        self.bid_book = Book(bid_comparators)

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
