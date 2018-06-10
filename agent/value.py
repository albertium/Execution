
import abc
from collections import namedtuple
from typing import List, Iterable
import numpy as np


StateSpec = namedtuple("StateSpec", ["lb", "ub", "num_of_tiles"])
TileSpec = namedtuple("TileSpec", ["lb", "ub", "start", "tile_width", "num_of_tiles"])


class ValueFunction:
    @abc.abstractmethod
    def __init__(self, state_spec: List[StateSpec]):
        pass

    @abc.abstractmethod
    def get_value(self, state: Iterable):
        pass

    @abc.abstractmethod
    def update_value(self, state: Iterable, value):
        pass


class Tiling(ValueFunction):
    def __init__(self, state_specs: List[StateSpec], offsets, num_tilings):
        super(Tiling, self).__init__(state_specs)
        self.values = np.zeros([spec.num_of_tiles + 1 for spec in state_specs])  # for tiling with offset, we need n + 1 tiles
        self.state_specs = []
        for spec, offset in zip(state_specs, offsets):
            tile_width = (spec.ub - spec.lb) / spec.num_of_tiles
            self.state_specs.append(
                TileSpec(lb=spec.lb,
                         ub=spec.ub,
                         start=spec.lb - tile_width * ((num_tilings - offset) % num_tilings) / num_tilings,
                         tile_width=tile_width,
                         num_of_tiles=spec.num_of_tiles)
            )
        self.num_states = len(state_specs)

    def get_value(self, state: Iterable):
        return self.values[self.get_indices(state)]

    def update_value(self, state: Iterable, value):
        self.values[self.get_indices(state)] += value

    def get_indices(self, state: Iterable):
        indices = []
        for spec, val in zip(self.state_specs, state):
            # this is a bit faster than applying min and max
            if val < spec.lb:
                val = spec.lb
            elif val >= spec.ub:
                val = spec.ub
            idx = int((val - spec.start) / spec.tile_width)
            indices.append(idx)
        return tuple(indices)


class TileCodingValueFunction(ValueFunction):
    def __init__(self, state_specs: List[StateSpec], num_tilings=None):
        super(TileCodingValueFunction, self).__init__(state_specs)
        self.num_states = len(state_specs)
        if num_tilings is None:
            self.num_tilings = 2 ** int(np.ceil(np.log2(4 * self.num_states)))  # set num of tilings to 2^m >= 4k
        else:
            self.num_tilings = num_tilings
        self.num_tiles = [spec.num_of_tiles for spec in state_specs]
        self.tilings = []
        offsets = np.array(range(1, 2 * self.num_states + 1, 2), dtype=int)
        for i in range(self.num_tilings):
            self.tilings.append(Tiling(state_specs, offsets * i, self.num_tilings))

    def get_value(self, state):
        value = 0
        for tiling in self.tilings:
            value += tiling.get_value(state)
        return value

    def update_value(self, state, value):
        delta = value / self.num_tilings
        for tiling in self.tilings:
            tiling.update_value(state, delta)
