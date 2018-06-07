
from sortedcollections import SortedList
from collections import OrderedDict, deque
from market.elements import Order, ExecutionInfo


class Book:
    """
    For order modification functions, we assume that the ref has been checked in the whole book level
    """
    def __init__(self, comparators):
        self.levels = SortedList(key=comparators[0])
        self.default_quote = 100000000 if comparators[0](1) == 1 else 0  # quote before book is initialized
        self.later_than = comparators[1]
        self.pool = {}  # record orders for update or deletion
        self.level_pool = {}  # store leveals
        self.volumes = OrderedDict()

    def __contains__(self, item):
        return item in self.pool

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

    def execute_order(self, ref, shares):
        # we assume that we never run out of limit orders
        # if ref is on the quote level and not the real front order, execute as it is
        # the remaining or the other cases are executed as incoming market order
        tmp = self.pool.get(ref, None)
        if tmp is not None and tmp.price == self.get_quote() and ref != self.get_front_real_order().ref:
            if tmp.shares > shares:
                tmp.shares -= shares
                self.update_volume(tmp.price, -shares)
                return [] # here we must have bid ref == ask ref and ref is real
            else:
                shares -= tmp.shares
                self.update_volume(tmp.price, -tmp.shares)
                self.remove(ref)
                self.update_book()
        return self.execute_market_market(ref, shares)

    def execute_market_market(self, ref, shares):
        # for message execution and market order execution
        # the function allows algo order on opposite sides
        executed = []
        while shares > 0:
            tmp = self.get_front_order()
            if tmp.shares <= shares:
                self.remove(tmp.ref)
                shares -= tmp.shares
                self.update_volume(tmp.price, -tmp.shares)
                self.update_book()
                if not tmp.real:
                    executed.append(ExecutionInfo(tmp.ref, tmp.price, tmp.shares))
                if ref < 0:
                    executed.append(ExecutionInfo(ref, tmp.price, tmp.shares))
            else:
                tmp.shares -= shares
                self.update_volume(tmp.price, -shares)
                if not tmp.real:
                    executed.append(ExecutionInfo(tmp.ref, tmp.price, shares))
                if ref < 0:
                    executed.append(ExecutionInfo(ref, tmp.price, shares))
                shares = 0
        return executed

    def cancel_order(self, ref, shares):
        tmp = self.pool.get(ref, None)
        if tmp is None:
            return
        if not tmp.valid:
            raise RuntimeError("Order cancellation error - order specs mismatch")
        if tmp.shares <= shares:
            self.remove(ref)
            self.update_volume(tmp.price, -tmp.shares)
            self.update_book()
        else:
            tmp.shares -= shares
            self.update_volume(tmp.price, -shares)

    def delete_order(self, ref):
        tmp = self.pool.get(ref, None)
        if tmp is None:
            return
        self.update_volume(tmp.price, -tmp.shares)
        self.remove(ref)
        self.update_book()

    def replace_order(self, ref, new_ref, price, shares):
        if ref not in self.pool:
            return
        self.delete_order(ref)
        self.add_order(new_ref, price, shares)
