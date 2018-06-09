
import abc
import numpy as np


class agent:
    def __init__(self, n_features, n_actions):
        self.n_features = n_features
        # self.action_space = list(range(n_actions))
        self.action_space = [8]

    @abc.abstractmethod
    def act(self, states):
        pass


class RandomAgent(agent):
    def __init__(self, n_features, n_actions):
        super(RandomAgent, self).__init__(n_features, n_actions)

    def act(self, states):
        return np.random.choice(self.action_space)
