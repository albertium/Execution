
from collections import namedtuple

ConfigClass = namedtuple("config", ["liquidation_rate", "target_size", "features", "delay_lb", "delay_ub", "skip_size"])
config = ConfigClass(liquidation_rate=0.3,
                     target_size=100,
                     skip_size=500,
                     features=["SPRD", "AVOL", "BVOL", "MPMV1"],
                     delay_lb=15000, delay_ub=25000)