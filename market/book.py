
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
        if ref < 0 or ref not in self.pool \
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
                    if tmp.ref != ref:  # the order would not be execute were it not for algo orders, save ref number
                        self.ref_pool[tmp.ref] = None  # use dict instead of set because it's faster
                else:
                    tmp.shares -= shares
                    self.update_volume(tmp.price, -shares)
                    shares = 0
                self.update_book()
                if ref < 0 or not tmp.real:  # report algo order execution for market or limit
                    executed.append(ExecutionInfo(ref, tmp.price, prev_shares - shares))
                elif not tmp.real:
                    executed.append(ExecutionInfo(tmp.ref, tmp.price, prev_shares - shares))

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

    def execute_market_market(self, ref, shares):
        # for message execution and market order execution
        executed = []
        while shares > 0:
            tmp = self.get_front_order()
            if tmp.shares <= shares:
                self.remove(tmp.ref)
                shares -= tmp.shares
                self.update_volume(tmp.price, -tmp.shares)
                self.update_book()
                if not tmp.real:  # we assume algo order shouldn't appear in both sides at the same time
                    executed.append(ExecutionInfo(tmp.ref, tmp.price, tmp.shares))
                elif ref < 0:
                    executed.append(ExecutionInfo(ref, tmp.price, tmp.shares))
                if tmp.ref == ref:
                    self.ref_pool[tmp.ref] = None  # use dict instead of set because it's faster
            else:
                tmp.shares -= shares
                self.update_volume(tmp.price, -shares)
                shares = 0
                if ref < 0 or not tmp.real:
                    executed.append(ExecutionInfo(tmp.ref, tmp.price, shares))
                elif ref < 0:
                    executed.append(ExecutionInfo(ref, tmp.price, shares))
        return executed

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
