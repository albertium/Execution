
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
        return self.ref + 1

    def add_market_order(self, shares, buy):
        last_transmission_time = self.messages[self.pointer - 1].timestamp
        self.last_delayed_time = max(self.last_delayed_time,
                                     last_transmission_time + np.random.uniform(self.delay_lb, self.delay_ub))
        msg = FormattedMessage()
        msg.type = 'M' + ('B' if buy else 'S')  # EA / EB is for real message
        msg.ref = self.ref
        msg.shares = shares
        self.open_orders.append(msg)
        self.ref -= 1
        return self.ref + 1

    def delete_order(self, ref, ask):
        last_transmission_time = self.messages[self.pointer - 1].timestamp
        self.last_delayed_time = max(self.last_delayed_time,
                                     last_transmission_time + np.random.uniform(self.delay_lb, self.delay_ub))
        msg = FormattedMessage()
        msg.type = 'D' + ('A' if ask else 'B')
        msg.ref = ref
        msg.timestamp = self.last_delayed_time
        self.open_orders.append(msg)


class Profile:
    def __init__(self, ask):
        self.prev_action = None
        self.quote_price = None
        self.submitted = 0
        self.orders = {}
        self.ask = ask


class SmartOrderRouter:
    def __init__(self, feed: Feed, order_book: OrderBook, env, target_size, alpha):
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

    def update_submission(self, ind, executed):
        orders = self.bid_profile.orders if ind == "B" else self.ask_profile.orders
        total_shares = 0
        for order in executed:
            if order.ref == -113:
                aaa = 1
            if orders[order.ref] == order.shares:
                del orders[order.ref]
            else:
                orders[order.ref] -= order.shares
            total_shares += order.shares
        if ind == "B":
            self.bid_profile.submitted -= total_shares
        else:
            self.ask_profile.submitted -= total_shares
        return total_shares

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
        quote_price = self.order_book.get_ask() if profile.ask else self.order_book.get_bid()
        if (profile.quote_price is not None and (quote_price - profile.quote_price) > 2000) \
                or action != profile.prev_action:
            for ref in profile.orders:
                self.feed.delete_order(ref, profile.ask)
            profile.orders.clear()
            profile.submitted = 0
        if profile.submitted == self.target_size:
            return
        if profile.ask:
            price = self.order_book.ask_book.levels[action - 1]  # add order to level 0 - 4
        else:
            price = self.order_book.bid_book.levels[action - 1]
        shares = self.target_size - profile.submitted
        profile.orders[self.feed.add_order(price, shares, profile.ask)] = shares
        profile.submitted = self.target_size
        profile.prev_action = action
        profile.quote_price = quote_price


class Simulator:
    def __init__(self, agent, filename, config):
        self.feed = Feed(filename, delay_lb=config.delay_lb, delay_ub=config.delay_ub)
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
        print("Build: start building book", end='', flush=True)
        while self.feed.has_next() and self.feed.peek().timestamp < 342E11:
            self.order_book.process_message(self.feed.next())
        print("\rBuild: finish building book")

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
        counter = 0
        a, b = 0, 0
        while self.feed.has_next():
            counter += 1
            action = self.agent.act(states)
            states, reward = self.step(action)
            if self.position == -200 and self.pnl == -10000:
                aaa = 1
            if counter % 10000 == 0:
                print("pnl: %4f / position: %d" % (self.pnl / 10000, self.position))

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
        tmp = self.feed.next()
        if tmp.ref == -18:
            aaa = 1
        ind, executed = self.order_book.process_message(tmp)

        # netting
        if ind is not None:
            shares_to_add = self.SOR.update_submission(ind, executed)  # reduce submission by the same amount
            if ind == 'B':
                self.open_buys.extend(executed)
            else:
                self.open_sells.extend(executed)
            while len(self.open_buys) > 0 and len(self.open_sells) > 0:
                if self.open_buys[0].shares == self.open_sells[0].shares:
                    self.pnl += self.open_buys[0].shares * (self.open_sells[0].price - self.open_buys[0].price)
                    self.open_buys.popleft()
                    self.open_sells.popleft()
                elif self.open_buys[0].shares > self.open_sells[0].shares:
                    self.pnl += self.open_sells[0].shares * (self.open_sells[0].price - self.open_buys[0].price)
                    self.open_sells.popleft()
                else:
                    self.pnl += self.open_buys[0].shares * (self.open_sells[0].price - self.open_buys[0].price)
                    self.open_buys.popleft()
            self.position += shares_to_add if ind == 'B' else -shares_to_add
        return self.update_states(), 0
