
# f))rom market.simulator import Simulator
# from agent.agent import RandomAgent
# from config import config
#
# agent = RandomAgent(1, 1)
# sim = Simulator(agent, "data/AAPL-20170102-v2.csv", config)
# sim.run_simulation()
# print(len(sim.order_book.ask_book.pool))
# print(len(sim.order_book.bid_book.pool

from agent.value import TileCodingValueFunction, StateSpec, ValueFunction
import matplotlib.pyplot as plt
import numpy as np
import random
from typing import List
# from utils.sutton import TilingsValueFunction
from time import clock


class MonteCarloTester:
    def __init__(self, funcs: List[ValueFunction], state_size):
        self.state_size = state_size
        self.funcs = funcs
        self.ans = np.linspace(-1, 1, state_size)

    def train(self, n=20):
        errs = []
        for i in range(len(self.funcs)):
            errs.append(np.zeros(n))
        for i in range(n):
            alpha = 1 / (i + 1)
            self.train_once(alpha)
            for idx, func in enumerate(self.funcs):
                values = [func.get_value([state]) for state in range(self.state_size)]
                errs[idx][i] = np.sqrt(np.mean(np.power(self.ans - values, 2)))
            print("%d: %f" % (i, errs[0][i]))
        # for err in errs:
        #     plt.plot(err)
        # plt.show()

    def train_once(self, alpha):
        history = [self.state_size // 2]
        while history[-1] not in [0, self.state_size - 1]:
            action = random.choice([1, -1])
            curr = history[-1]
            curr += random.randint(1, int(self.state_size / 10)) * action
            if curr < 0:
                curr = 0
            elif curr >= self.state_size:
                curr = self.state_size - 1
            history.append(curr)

        reward = -1 if history[-1] == 0 else 1
        for state in history:
            for func in self.funcs:
                func.update_value([state], alpha * (reward - func.get_value([state])))


funcs = [TileCodingValueFunction([StateSpec(lb=0, ub=999, num_of_tiles=5)], 50)]
         # TilingsValueFunction(50, 200, 4)]
         # TileCodingValueFunction([StateSpec(lb=0, ub=999, num_of_tiles=50)], 1)]
tester = MonteCarloTester(funcs, 1000)
start = clock()
tester.train(300)
print("%ds" % (clock() - start))
