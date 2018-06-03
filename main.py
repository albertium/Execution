
from utils.checks import check_book
from market.environment import Simulation
from agent.agent import RandomAgent
from config import config

check_book("data/AAPL-20170102.csv")
# agent = RandomAgent(1, 1)
# sim = Simulation(agent, "data/AAPL-20170102.csv", config)
# sim.run_simulation()


# from utils.parse2 import parse_and_save
# parse_and_save("data/S020117-v50.txt", "output/AAPL-20170102.csv")
