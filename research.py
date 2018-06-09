
from market.simulator import Feed
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

feed = Feed("data/AAPL-20170102.csv")

prev_timestamp = None
output = []
while feed.has_next():
    order = feed.next()
    if 342E11 < order.timestamp < 576E11:
        if prev_timestamp is not None:
            output.append(order.timestamp - prev_timestamp)
        prev_timestamp = order.timestamp

output = [x for x in output if x < 80000]
sim = np.random.exponential(19500, len(output))
print(np.mean(output))
pd.DataFrame({'delta': output, "sim": sim}).plot.hist(bins=1000)
plt.show()
