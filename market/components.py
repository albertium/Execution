
import csv
import numpy as np
from collections import deque
from market.elements import FormattedMessage, ExecutionInfo
from market.order_book import OrderBook


class Profile:
    def __init__(self, ask):
        self.submitted = 0
        self.orders = {}
        self.ask = ask


class Feed:
    def __init__(self, filename, delay_lb=1500, delay_ub=3000):
        self.messages = []
        self.pointer = 0
        self.open_orders = deque()
        self.last_transmission_time = 0  # to simulate delayed transmission
        self.last_wall_time = 0
        self.wall_time = 0
        self.ref = -1
        self.delay_lb = delay_lb
        self.delay_ub = delay_ub
        print("Feed: reading data", end='', flush=True)
        with open(filename, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                self.messages.append(FormattedMessage(row))
        print("\rFeed: finish parsing message data")
        self.size = len(self.messages)

    def has_next(self):
        return self.pointer < self.size

    def next(self):
        if len(self.open_orders) > 0 and self.messages[self.pointer].timestamp > self.open_orders[0].timestamp:
            tmp = self.open_orders.popleft()
        else:
            tmp = self.messages[self.pointer]
            self.pointer += 1
        self.wall_time = tmp.timestamp
        return tmp

    def time(self):
        if self.last_wall_time < self.wall_time:
            self.last_transmission_time = max(self.last_transmission_time,
                                              self.wall_time + np.random.uniform(self.delay_lb, self.delay_ub))
            self.last_wall_time = self.wall_time
        else:
            self.last_transmission_time += 500
        return self.last_transmission_time

    def peek(self):
        return self.messages[self.pointer]

    def add_order(self, price, shares, ask=True):
        msg = FormattedMessage()
        msg.type = 'AA2' if ask else 'AB2'
        msg.ref = self.ref
        msg.timestamp = self.time()
        msg.price = price
        msg.shares = shares
        self.open_orders.append(msg)
        self.ref -= 1
        return self.ref + 1

    def add_market_order(self, shares, buy):
        msg = FormattedMessage()
        msg.type = 'M' + ('B' if buy else 'S')  # EA / EB is for real message
        msg.ref = self.ref
        msg.timestamp = self.time()
        msg.shares = shares
        self.open_orders.append(msg)
        self.ref -= 1
        return self.ref + 1

    def delete_order(self, ref, ask):
        msg = FormattedMessage()
        msg.type = 'D' + ('A' if ask else 'B')
        msg.ref = ref
        msg.timestamp = self.time()
        self.open_orders.append(msg)


class SmartOrderRouter:
    def __init__(self, feed: Feed, order_book: OrderBook, env, target_size, alpha, skip_size):
        """
        feed is for executing orders while order_book is only for info
        """
        self.feed = feed
        self.order_book = order_book
        self.env = env
        self.ask_profile = Profile(ask=True)
        self.bid_profile = Profile(ask=False)
        self.action_map = {0: (1, 1), 1: (2, 2), 2: (3, 3), 3: (4, 4), 4: (5, 5), 5: (1, 3), 6: (3, 1),
                           7: (2, 5), 8: (5, 2)}
        self.position = 0
        self.target_size = target_size  # size to maintain on each book
        self.alpha = alpha  # liquidation percentage
        self.skip_size = skip_size  # for level re-anchoring

    def update_submission(self, ind, executed):
        orders = self.bid_profile.orders if ind == "B" else self.ask_profile.orders
        queue_shares = 0
        delete_shares = 0
        for info in executed:
            if info.ref in orders:
                if orders[info.ref].shares == info.shares:
                    del orders[info.ref]
                else:
                    orders[info.ref].shares -= info.shares
                queue_shares += info.shares
            else:
                delete_shares += info.shares
        if ind == "B":
            self.bid_profile.submitted -= queue_shares
        else:
            self.ask_profile.submitted -= queue_shares
        return queue_shares + delete_shares

    def execute(self, action):
        if action == 9:
            if self.position > 0:
                orders, queue, ask = self.bid_profile.orders, self.ask_profile.orders, False
            else:
                orders, queue, ask = self.ask_profile.orders, self.bid_profile.orders, True
            for order in orders:
                self.feed.delete_order(order.ref, ask)
            shares = int(self.alpha * self.position)
            queue[self.feed.add_market_order(shares, self.position < 0)] = shares
        else:
            self.execute_single_book(self.action_map[action][0], self.ask_profile)
            self.execute_single_book(self.action_map[action][1], self.bid_profile)

    def execute_single_book(self, action, profile: Profile):
        # if action changed, clear all previous orders
        if profile.ask:
            target_price = self.order_book.get_real_ask(action - 1)
        else:
            target_price = self.order_book.get_real_bid(action - 1)

        # clear stalled orders
        to_delete = []
        for ref, info in profile.orders.items():
            if abs(info.price - target_price) > self.skip_size:
                self.feed.delete_order(ref, profile.ask)
                profile.submitted -= info.shares
                to_delete.append(ref)
        for ref in to_delete:
            del profile.orders[ref]

        # refill order if needed
        if profile.submitted < self.target_size:
            shares = self.target_size - profile.submitted
            ref = self.feed.add_order(target_price, shares, profile.ask)
            profile.orders[ref] = ExecutionInfo(ref, target_price, shares)
            profile.submitted = self.target_size
