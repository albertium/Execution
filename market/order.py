
from collections import deque


class Order:
    def __init__(self, ref, price, shares):
        self.ref = ref
        self.price = price
        self.shares = shares
        self.valid = True
        self.generated = True  # the order is user generated or from historical data
