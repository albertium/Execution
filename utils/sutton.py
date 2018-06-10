
import numpy as np
from matplotlib import pyplot as plt
from time import clock


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
        stateValue = 0.0
        # go through all the tilings
        for tilingIndex in range(0, len(self.tilings)):
            # find the active tile in current tiling
            tileIndex = (state - self.tilings[tilingIndex]) // self.tileWidth
            stateValue += self.params[tilingIndex, tileIndex]
        return stateValue

    def update_value(self, state, delta):

        # each state is covered by same number of tilings
        # so the delta should be divided equally into each tiling (tile)
        delta /= self.numOfTilings

        # go through all the tilings
        for tilingIndex in range(0, len(self.tilings)):
            # find the active tile in current tiling
            tileIndex = (state - self.tilings[tilingIndex]) // self.tileWidth
            self.params[tilingIndex, tileIndex] += delta


def getAction():
    if np.random.binomial(1, 0.5) == 1:
        return 1
    return -1


def takeAction(state, action):
    step = np.random.randint(1, 101)
    step *= action
    state += step
    state = max(min(state, 1001), 0)
    if state == 0:
        reward = -1
    elif state == 1000 + 1:
        reward = 1
    else:
        reward = 0
    return state, reward


def gradientMonteCarlo(valueFunction, alpha, distribution=None):
    currentState = 500
    trajectory = [currentState]

    # We assume gamma = 1, so return is just the same as the latest reward
    reward = 0.0
    while currentState not in [0, 1001]:
        action = getAction()
        newState, reward = takeAction(currentState, action)
        trajectory.append(newState)
        currentState = newState

    # Gradient update for each state in this trajectory
    for state in trajectory[:-1]:
        delta = alpha * (reward - valueFunction.get_value(state))
        valueFunction.update_value(state, delta)
        if distribution is not None:
            distribution[state] += 1

trueStateValues = np.arange(-1001, 1003, 2) / 1001.0


def figure9_10():

    # number of episodes
    episodes = 1000
    numOfTilings = 50
    tileWidth = 200
    tilingOffset = 4
    runs = 1

    labels = ['tile coding (50 tilings)']

    # track errors for each episode
    errors = np.zeros((len(labels), episodes))
    for run in range(0, runs):
        # initialize value functions for multiple tilings and single tiling
        valueFunctions = [TilingsValueFunction(numOfTilings, tileWidth, tilingOffset)]
        for i in range(0, len(valueFunctions)):
            for episode in range(0, episodes):

                # I use a changing alpha according to the episode instead of a small fixed alpha
                # With a small fixed alpha, I don't think 5000 episodes is enough for so many
                # parameters in multiple tilings.
                # The asymptotic performance for single tiling stays unchanged under a changing alpha,
                # however the asymptotic performance for multiple tilings improves significantly
                alpha = 1.0 / (episode + 1)

                # gradient Monte Carlo algorithm
                gradientMonteCarlo(valueFunctions[i], alpha)

                # get state values under current value function
                stateValues = [valueFunctions[i].get_value(state) for state in np.arange(1, 1001)]

                # get the root-mean-squared error
                errors[i][episode] += np.sqrt(np.mean(np.power(trueStateValues[1: -1] - stateValues, 2)))
                print('run:', run, 'episode:', episode, "err:", errors[i][episode])

    # average over independent runs
    errors /= runs

    # plt.figure(4)
    # for i in range(0, len(labels)):
    #     plt.plot(errors[i], label=labels[i])
    # plt.xlabel('Episodes')
    # plt.ylabel('RMSVE')
    # plt.legend()
    # plt.show()


start = clock()
figure9_10()
print("%ds" % (clock() - start))