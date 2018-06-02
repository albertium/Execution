
import csv
import abc
from collections import deque
from market.order_book import OrderBook, SimulationBook


class RollingFeature:
    @abc.abstractmethod
    def add(self, val):
        # should return new value add the same time
        pass

    @abc.abstractmethod
    def get(self):
        pass


class RollingMean(RollingFeature):
    def __init__(self, alpha=0.95):
        self.alpha = (alpha, 1 - alpha)
        self.mean = None

    def add(self, val):
        if self.mean is None:
            self.mean = val
        else:
            self.mean = self.alpha[0] * self.mean + self.alpha[1] * val

    def get(self):
        return self.mean


class RollingVariance(RollingFeature):
    def __init__(self, alpha=0.95):
        self.alpha = (alpha, 1 - alpha)
        self.mean = None
        self.var = None

    def add(self, val):
        if self.mean is None:
            self.mean = val
            self.var = 0
        else:
            self.mean = self.alpha[0] * self.mean + self.alpha[1] * val
            self.var = self.alpha[0] * self.var + self.alpha[1] * pow(val - self.mean, 2)

    def get(self):
        return self.var


class FeatureDelta(RollingFeature):
    def __init__(self, n=1):
        self.n = n
        self.records = deque()
        self.delta = 0

    def add(self, val):
        self.records.append(val)
        if len(self.records) > self.n:
            self.records.popleft()
        self.delta = self.records[-1] - self.records[0]
        return self.delta

    def get(self):
        return self.delta


class Feed:
    def __init__(self, filename):
        self.messages = []
        self.pointer = 0
        self.open_orders = deque()
        with open(filename, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                self.messages.append(row)
        self.size = len(self.messages)

    def has_next(self):
        return self.pointer < self.size

    def next(self):
        pass

    def add(self):
        pass


class Simulation:
    def __init__(self, agent, filename, config):
        self.feed = Feed(filename)
        self.order_book = OrderBook(SimulationBook)  # SimulationBook allow algo generated orders
        self.agent = agent
        self.config = config
        self.build_book()
        self.init_features()

    def build_book(self):
        """
        At the beginning of the day, Nasdaq will populate the whole book by sending "add" message
        """
        while self.messages[self.pointer][0] == 'A':
            self.order_book.process_message(self.messages[self.pointer])
            self.pointer += 1

    def init_features(self):
        # initialize features
        for name in self.config.features:
            if name[:4] == "MPMV":
                lag = int(name[4:])
                self.mpmv = FeatureDelta(lag)  # mid price move

    def run_simulation(self):
        states = self.get_state()
        while self.feed.has_next():
            action = self.agent.act(states)
            states, reward = self.step(action, self.feed.next())

    def get_state(self):
        states = []
        for name in self.config.features:
            tmp = name[:4]
            if tmp == "SPRD":
                states.append(self.order_book.get_spread())
            elif tmp == "BVOL":
                states.append(self.order_book.bid_book.get_quote_volume())
            elif tmp == "AVOL":
                states.append(self.order_book.ask_book.get_quote_volume())
            elif tmp == "MPMV":
                # mid price move
                states.append(self.mpmv.add(self.order_book.get_mid_price()))
        return states

    def step(self, action, msg):
        return None, None