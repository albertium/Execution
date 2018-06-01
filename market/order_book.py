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


class Wrapper:
    def __init__(self):
        self.price = None
wrapper = Wrapper()


ask_comparators = [lambda x: x.price, lambda x, y: x > y]
bid_comparators = [lambda x: -x.price, lambda x, y: x < y]


class Book:
    """
    For order modification functions, we assume that the ref has been checked in the whole book level
    """
    def __init__(self, comparators=ask_comparators):
        self.levels = SortedList(key=comparators[0])
        self.later_than = comparators[1]
        self.pool = {}  # record orders for update or deletion
        self.ref_price = None  # best bid or offer

    def __contains__(self, item):
        return item in self.pool

    def get_foremost_order(self):
        return self.levels[0].first()

    def remove(self, ref):
        tmp = self.pool.pop(ref)
        tmp.valid = False

    def update_book(self):
        while not self.get_foremost_order().valid:
            self.levels[0].popleft()
            if self.levels[0].is_empty():
                self.levels.pop(0)
        self.ref_price = self.levels[0].price

    def add_order(self, ref, price: float, shares):
        order = Order(ref, price, shares)
        self.pool[ref] = order
        try:
            wrapper.price = price
            level = self.levels.index(wrapper)
            level.append(order)
        except ValueError:  # when level above not found and return None
            self.levels.add(Level(price, order))

    def execute_order(self, ref, shares):
        self.update_book()
        tmp = self.pool[ref]
        if self.later_than(tmp.price, self.ref_price):
            raise RuntimeError("Order execution error - execution not on the best level")
        if tmp.shares < shares:
            raise RuntimeError("Order execution error - executed more than available")
        elif tmp.shares > shares:
            tmp.shares -= shares
        else:
            self.remove(ref)
        self.update_book()

    def execute_order_with_price(self, ref, price, shares):
        self.execute_order(ref, shares)

    def cancel_order(self, ref, shares):
        tmp = self.pool[ref]

        if not tmp.valid or tmp.shares < shares:
            raise RuntimeError("Order cancellation error - order specs mismatch")
        tmp.shares -= shares

    def delete_order(self, ref):
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

    def get_foremost_real_order(self):
        """
        the foremost real (not generated) order
        every order related function should call update_book at the end. We shouldn't need to do it again here
        """
        for level in self.levels:
            for order in level:
                if order.real:
                    return order
        return None

    def execute_order(self, ref, shares):
        self.update_book()
        tmp = self.pool[ref]
        # if the target order is not on the first level or execution is on the original foremost order
        # then we should walk the book starting from the foremost order
        if self.later_than(tmp.price, self.ref_price) or ref == self.get_foremost_real_order().ref:
            while shares > 0:
                tmp = self.get_foremost_order()
                if tmp.shares <= shares:
                    self.remove(tmp.ref)
                    shares -= tmp.shares
                    if tmp.ref != ref:
                        self.ref_pool[tmp.ref] = None  # use dcit instead of set because it's faster
                else:
                    tmp.shares -= shares
                    shares = 0
                self.update_book()
            self.ref_pool.pop(ref, None)  # it's possible that ref is a ref_pool order
        else:
            tmp.shares -= shares
            if tmp.shares == 0:
                self.remove(ref)
        self.update_book()


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


class SimulationOrderBook(OrderBook):
    def __init__(self):
        super(SimulationOrderBook, self).__init__()
        self.ask_book = SimulationBook(ask_comparators)
        self.bid_book = SimulationBook(bid_comparators)
