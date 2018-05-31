
# from sortedcontainers import SortedList
from collections import deque
from market.order import Order


def ask_comparator(x: float, y: float):
    return x > y


def bid_comparator(x: float, y: float):
    return x < y


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


class Book:
    """
    linear in the first nth element and heap afterward
    For order modification functions, we assume that the ref has been checked in the whole book level
    """
    def __init__(self, later_than=ask_comparator):
        self.later_than = later_than
        self.levels = deque()
        self.pool = {}  # record orders for update or deletion
        self.ref_price = None  # best bid or offer

    def __contains__(self, item):
        return item in self.pool

    def update_book(self):
        while not self.levels[0].first().valid:
            self.levels[0].popleft()
            if self.levels[0].is_empty():
                self.levels.popleft()
        self.ref_price = self.levels[0].price

    def add_order(self, ref, price: float, shares):
        order = Order(ref, price, shares)
        self.pool[ref] = order
        if len(self.levels) == 0:  # corner case
            self.levels.append(Level(price, order))
            return

        for idx, level in enumerate(self.levels):
            if not self.later_than(price, level.price):
                if price == level.price:
                    level.append(order)
                else:
                    self.levels.insert(idx, Level(price, order))
                break
        self.levels.append(Level(price, order))  # farthest from the market

    def execute_order(self, ref, shares):
        self.update_book()
        if self.levels[0].first().ref != ref:
            raise RuntimeError("Order execution error - mismatch reference")
        tmp = self.pool[ref]
        if tmp.shares < shares:
            raise RuntimeError("Order execution error - executed more than available")
        tmp.shares -= shares
        if tmp.shares == 0:
            tmp.valid = False
            del self.pool[ref]

    def execute_order_with_price(self, ref, price, shares):
        # this is the same as order cancellation
        tmp = self.pool[ref]
        if not tmp.valid or tmp.shares < shares:
            raise RuntimeError("Order execution error - order specs mismatch")
        tmp.shares -= shares
        if tmp.shares == 0:
            tmp.valid = False
        print("order executed at %d with original display %d" % (price, self.pool[ref].price))

    def cancel_order(self, ref, shares):
        tmp = self.pool[ref]
        if not tmp.valid or tmp.shares < shares:
            raise RuntimeError("Order cancellation error - order specs mismatch")
        tmp.shares -= shares
        if tmp.shares == 0:
            print("cancel to 0\n")
            tmp.valid = False

    def delete_order(self, ref):
        self.pool[ref].valid = False

    def replace_order(self, ref, new_ref, price, shares):
        self.delete_order(ref)
        self.add_order(new_ref, price, shares)


class OrderBook:
    def __init__(self):
        self.ask_book = Book(ask_comparator)
        self.bid_book = Book(bid_comparator)

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
