
from market.simulator import Simulator
from agent.agent import RandomAgent
from config import config

agent = RandomAgent(1, 1)
sim = Simulator(agent, "data/AAPL-20170102-v2.csv", config)
sim.run_simulation()
print(len(sim.order_book.ask_book.pool))
print(len(sim.order_book.bid_book.pool))