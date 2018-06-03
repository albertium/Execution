
from collections import namedtuple

ConfigClass = namedtuple("config", ["liquidation_rate", "target_size", "features"])
config = ConfigClass(liquidation_rate=0.3, target_size=500, features=["SPRD", "AVOL", "BVOL", "MPMV1"])