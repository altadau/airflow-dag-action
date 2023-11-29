import os
import sys

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================


class LeucipaClusterSize:

    def __init__(self,
                 suffix: str,
                 master_nodes: int,
                 core_nodes: int,
                 task_nodes: int):
        self.suffix = suffix
        self.master_nodes = master_nodes
        self.core_nodes = core_nodes
        self.task_nodes = task_nodes


class LeucipaClusterSizes:
    SMALL = LeucipaClusterSize(suffix="S", master_nodes=1, core_nodes=2, task_nodes=2)
    MEDIUM = LeucipaClusterSize(suffix="M", master_nodes=1, core_nodes=2, task_nodes=8)
    LARGE = LeucipaClusterSize(suffix="L", master_nodes=1, core_nodes=4, task_nodes=16)
    XL = LeucipaClusterSize(suffix="XL", master_nodes=1, core_nodes=8, task_nodes=32)
    XXL = LeucipaClusterSize(suffix="XXL", master_nodes=1, core_nodes=16, task_nodes=64)
    XXXL = LeucipaClusterSize(suffix="XXXL", master_nodes=1, core_nodes=32, task_nodes=128)
