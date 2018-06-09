
from collections import deque
from time import clock
from market.order_book import OrderBook
from market.components import Feed, SmartOrderRouter
from utils.feature import FeatureDelta, RollingMean


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
        self.counter = 0
        self.build_book()
        self.init_features()
        # rspd only exists after init_features()
        self.SOR = SmartOrderRouter(self.feed, self.order_book, self, config.target_size, config.liquidation_rate,
                                    config.skip_size)

    def build_book(self):
        """
        At the beginning of the day, Nasdaq will populate the whole book by sending "add" message
        """
        print("Build: start building book", end='', flush=True)
        while self.feed.has_next() and self.feed.peek().timestamp < 342E11:
            self.order_book.process_message(self.feed.next())
            self.counter += 1
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
        start = clock()
        while self.feed.has_next():
            self.counter += 1
            action = self.agent.act(states)
            states, reward = self.step(action)
            if self.counter % 10000 == 0:
                print("pnl: %.2f / position: %d" % (self.pnl / 10000, self.position))
        print("%ds / %d records (%.2f)" % (clock() - start, self.counter - len(self.feed.messages),
                                           self.counter / len(self.feed.messages) * 100 ))

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
        ind, executed = self.order_book.process_message(self.feed.next())

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
