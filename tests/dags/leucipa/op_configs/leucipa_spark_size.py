import os
import sys

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================


class LeucipaSparkSize:

    def __init__(self,
                 suffix: str,
                 master_nodes: int,
                 core_nodes: int,
                 task_nodes: int,
                 driver_memory: str = "8g"):
        self.suffix = suffix
        self.master_nodes = master_nodes
        self.core_nodes = core_nodes
        self.task_nodes = task_nodes
        self.driver_memory = driver_memory

    def get_spark_conf(self):
        return {
            "spark.driver.memory": self.driver_memory,  # (<< 12g)
            "spark.executor.memory": "10g",  # (<< 12g)
            "spark.executor.cores": 3,  # (== 1)
            # "spark.executor.instances": (self.core_nodes + self.task_nodes) * 4 - 1,
            "spark.default.parallelism": (self.core_nodes + self.task_nodes) * 4 * 3,
            "spark.sql.shuffle.partitions": (self.core_nodes + self.task_nodes) * 4 * 3 * 5,
            "spark.yarn.executor.memoryOverhead": "1g",  # (<< 2g)
        }


class LeucipaSparkSizes:
    SMALL = LeucipaSparkSize(suffix="S", master_nodes=1, core_nodes=2, task_nodes=2)
    MEDIUM = LeucipaSparkSize(suffix="M", master_nodes=1, core_nodes=2, task_nodes=8)
    LARGE = LeucipaSparkSize(suffix="L", master_nodes=1, core_nodes=4, task_nodes=16)
    XL = LeucipaSparkSize(suffix="XL", master_nodes=1, core_nodes=8, task_nodes=32)
    XXL = LeucipaSparkSize(suffix="XXL", master_nodes=1, core_nodes=16, task_nodes=64)
    XXXL = LeucipaSparkSize(suffix="XXXL", master_nodes=1, core_nodes=32, task_nodes=128)
