
import csv
from collections import deque, namedtuple
import numpy as np
from market.order_book import OrderBook, FormattedMessage
from utils.feature import FeatureDelta, RollingMean


class Feed:
    def __init__(self, filename, delay_lb=1500, delay_ub=3000):
        self.messages = []
        self.pointer = 0
        self.open_orders = deque()
        self.last_delayed_time = 0  # to simulate delayed transmission
        self.ref = 0
        self.delay_lb = delay_lb
        self.delay_ub = delay_ub
        print("Feed: reading data")
        with open(filename, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                self.messages.append(FormattedMessage(row))
        print("Feed: finish parsing message data")
        self.size = len(self.messages)

    def has_next(self):
        return self.pointer < self.size

    def next(self):
        if len(self.open_orders) > 0 and self.messages[self.pointer].timestamp > self.open_orders[0].timestamp:
            return self.open_orders.popleft()
        else:
            tmp = self.messages[self.pointer]
            self.pointer += 1
            return tmp

    def peek(self):
        return self.messages[self.pointer]

    def add_order(self, price, shares, ask=True):
        last_transmission_time = self.messages[self.pointer - 1].timestamp
        self.last_delayed_time = max(self.last_delayed_time,
                                     last_transmission_time + np.random.uniform(self.delay_lb, self.delay_ub))
        msg = FormattedMessage()
        msg.type = 'AA2' if ask else 'AB2'
        msg.ref = self.ref
        msg.timestamp = self.last_delayed_time
        msg.price = price
        msg.shares = shares
        self.open_orders.append(msg)
        self.ref -= 1

    def delete_order(self, ref):
        last_transmission_time = self.messages[self.pointer - 1].timestamp
        self.last_delayed_time = max(self.last_delayed_time,
                                     last_transmission_time + np.random.uniform(self.delay_lb, self.delay_ub))
        msg = FormattedMessage()
        msg.type = 'D'
        msg.ref = ref
        msg.timestamp = self.last_delayed_time
        self.open_orders.append(msg)

    def add_market_order(self, shares, ask=True):
        last_transmission_time = self.messages[self.pointer - 1].timestamp
        self.last_delayed_time = max(self.last_delayed_time,
                                     last_transmission_time + np.random.uniform(self.delay_lb, self.delay_ub))
        msg = FormattedMessage()
        msg.type = 'E'
        msg.ref = -1 if ask else -2
        msg.shares = shares
        self.open_orders.append(msg)


class SmartOrderRouter:
    def __init__(self, feed: Feed, order_book: OrderBook, env, target_size, alpha):
        """
        feed is for executing orders while order_book is only for info
        """
        self.feed = feed
        self.order_book = order_book
        self.env = env
        self.ask_profile = {"prev_action": None, "remaining": 0, "refs": [], "ask": True}
        self.bid_profile = {"prev_action": None, "remaining": 0, "refs": [], "ask": False}
        self.action_map = {0: (1, 1), 1: (2, 2), 2: (3, 3), 3: (4, 4), 4: (5, 5), 5: (1, 3), 6: (3, 1),
                           7: (2, 5), 8: (5, 2)}
        self.position = 0
        self.target_size = target_size  # size to maintain on each book
        self.alpha = alpha  # liquidation percentage

    def execute(self, action):
        if action == 9:
            refs = self.bid_profile["refs"] if self.position > 0 else self.ask_profile["refs"]
            for ref in refs:
                self.feed.delete_order(ref)
            # if position > 0, wee need to sell and execute in the BID book. Vice versa
            self.feed.add_market_order(int(self.alpha * self.position), self.position < 0)
        else:
            self.execute_single_book(self.action_map[action][0], self.ask_profile)
            self.execute_single_book(self.action_map[action][1], self.bid_profile)

    def execute_single_book(self, action, profile):
        # if action changed, clear all previous orders
        if profile["prev_action"] is not None and profile["prev_action"] != action:
            for ref in profile["refs"]:
                self.feed.delete_order(ref)
                profile["remaining"] = 0
        if profile["ask"]:
            price = max(self.order_book.get_mid_price() + self.env.rspd.get(),
                        self.order_book.ask_book.get_quote())  # not to place inside the market
        else:
            price = min(self.order_book.get_mid_price() - self.env.rspd.get(),
                        self.order_book.bid_book.get_quote())  # not to place inside the market
        self.feed.add_order(price, self.target_size - profile["remaining"], profile["ask"])
        profile["prev_action"] = action


class Simulation:
    def __init__(self, agent, filename, config):
        self.feed = Feed(filename)
        self.order_book = OrderBook()  # SimulationBook allow algo generated orders
        self.agent = agent
        self.config = config
        self.default_features = ["MSPD50"]
        self.open_buys, self.open_sells = deque(), deque()
        self.position = 0
        self.pnl = 0
        self.build_book()
        self.init_features()
        # rspd only exists after init_features()
        self.SOR = SmartOrderRouter(self.feed, self.order_book, self, config.target_size, config.liquidation_rate)

    def build_book(self):
        """
        At the beginning of the day, Nasdaq will populate the whole book by sending "add" message
        """
        while self.feed.has_next() and self.feed.peek().type[0] == 'A':
            self.order_book.process_message(self.feed.next())

    def init_features(self):
        # initialize features
        for name in set(self.config.features + self.default_features):
            if name[:4] == "MPMV":
                lag = int(name[4:])
                self.mpmv = FeatureDelta(lag)  # mid price move
            if name[:4] == "MSPD":
                alpha = float(name[4:]) / 100
                self.rspd = RollingMean(alpha)

    def run_simulation(self):
        states = self.update_states()
        while self.feed.has_next():
            action = self.agent.act(states)
            states, reward = self.step(action)

    def update_states(self):
        states = []
        for name in self.config.features:
            states.append(self._update_states(name))
        for name in set(self.default_features) - set(self.config.features):
            self._update_states(name)
        return states

    def _update_states(self, name):
        tmp = name[:4]
        if tmp == "SPRD":
            return self.order_book.get_spread()
        elif tmp == "BVOL":
            return self.order_book.bid_book.get_quote_volume()
        elif tmp == "AVOL":
            return self.order_book.ask_book.get_quote_volume()
        elif tmp == "MPMV":
            # mid price move
            return self.mpmv.add_and_get(self.order_book.get_mid_price())
        elif tmp == "MSPD":  # moving spread
            return self.rspd.add_and_get(self.order_book.get_spread())

    def step(self, action):
        self.SOR.execute(action)
        buy, executed = self.order_book.process_message(self.feed.next())

        # netting
        if buy is not None:
            if buy:
                self.open_buys.extend(executed)
            else:
                self.open_sells.extend(executed)
            shares_to_add = 0
            while len(self.open_buys) > 0 and len(self.open_sells) > 0:
                if self.open_buys[0][1] >= self.open_sells[0][1]:
                    tmp, remain = self.open_sells.popleft(), self.open_buys[0]
                    self.pnl += tmp[1] * (tmp[0] - remain[0])
                else:
                    tmp, remain = self.open_buys.popleft(), self.open_sells[0]
                    self.pnl += tmp[1] * (remain[0] - tmp[0])
                shares_to_add += tmp[1]
                remain[1] -= tmp[1]
                if self.open_buys[0][1] == 0:  # >= condition is on open_buys side
                    self.open_buys.popleft()
            self.position += shares_to_add if buy else -shares_to_add

        return self.update_states(), 0
