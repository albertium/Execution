
import numpy as np
import random


class TilingsValueFunction:
    def __init__(self, numOfTilings, tileWidth, tilingOffset):
        self.numOfTilings = numOfTilings
        self.tileWidth = tileWidth
        self.tilingOffset = tilingOffset

        self.tilingSize = 1000 // tileWidth + 1

        self.params = np.zeros((self.numOfTilings, self.tilingSize))

        # For performance, only track the starting position for each tiling
        # As we have one more tile for each tiling, the starting position will be negative
        self.tilings = np.arange(-tileWidth + 1, 0, tilingOffset)

    def get_value(self, state):
        state = state[0]
        stateValue = 0.0
        # go through all the tilings
        for tilingIndex in range(0, len(self.tilings)):
            # find the active tile in current tiling
            tileIndex = (state - self.tilings[tilingIndex]) // self.tileWidth
            stateValue += self.params[tilingIndex, tileIndex]
        return stateValue

    def update_value(self, state, delta):
        state = state[0]
        # each state is covered by same number of tilings
        # so the delta should be divided equally into each tiling (tile)
        delta /= self.numOfTilings

        # go through all the tilings
        for tilingIndex in range(0, len(self.tilings)):
            # find the active tile in current tiling
            tileIndex = (state - self.tilings[tilingIndex]) // self.tileWidth
            self.params[tilingIndex, tileIndex] += delta


class MonteCarloTester:
    def __init__(self, funcs, end_state):
        self.end_state = end_state
        self.funcs = funcs
        self.ans = np.linspace(-1, 1, end_state + 1)[1: -1]
        self.errs = []

    def train(self, n=20):
        for i in range(len(self.funcs)):
            self.errs.append(np.zeros(n))
        for i in range(n):
            alpha = 1 / (i + 1)
            self.train_once(alpha)
            for idx, func in enumerate(self.funcs):
                values = [func.get_value([state]) for state in range(1, self.end_state)]
                self.errs[idx][i] = np.sqrt(np.mean(np.power(self.ans - values, 2)))

    def train_once(self, alpha):
        history = [self.end_state // 2]
        while history[-1] not in [0, self.end_state]:
            action = random.choice([1, -1])
            curr = history[-1]
            curr += random.randint(1, int(self.end_state / 10)) * action
            if curr < 0:
                curr = 0
            elif curr > self.end_state:
                curr = self.end_state
            history.append(curr)

        reward = -1 if history[-1] == 0 else 1
        for state in history[:-1]:
            for func in self.funcs:
                func.update_value([state], alpha * (reward - func.get_value([state])))
