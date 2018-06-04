
from market.environment import Simulation
from agent.agent import RandomAgent
from config import config

agent = RandomAgent(1, 1)
sim = Simulation(agent, "data/AAPL-20170102.csv", config)
sim.run_simulation()
