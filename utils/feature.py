
import abc
from collections import deque


class RollingFeature:
    @abc.abstractmethod
    def add_and_get(self, val):
        # should return new value add the same time
        pass

    @abc.abstractmethod
    def get(self):
        pass


class RollingMean(RollingFeature):
    def __init__(self, alpha=0.95):
        self.alpha = (alpha, 1 - alpha)
        self.mean = None

    def add_and_get(self, val):
        if self.mean is None:
            self.mean = val
        else:
            self.mean = self.alpha[0] * self.mean + self.alpha[1] * val
        return self.mean

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

    def add_and_get(self, val):
        self.records.append(val)
        if len(self.records) > self.n:
            self.records.popleft()
        self.delta = self.records[-1] - self.records[0]
        return self.delta

    def get(self):
        return self.delta