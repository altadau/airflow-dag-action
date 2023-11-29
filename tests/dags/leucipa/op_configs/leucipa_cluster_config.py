import os
import sys
from typing import Dict, List

from airflow.models import Variable

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================

from leucipa.utils.asset_util import AssetUtil
from leucipa.op_configs.leucipa_cluster_size import LeucipaClusterSize
from leucipa.constants.airflow_variables import *

DEFAULT_BOOTSTRAP_SCRIPTS = {
    "Dependencies": "assets://bootstrap_actions/dependencies.sh"
}


class LeucipaClusterConfig:
    """
    EMR Cluster config, aimed for Spark.
    """

    def __init__(self,
                 name: str,
                 cluster_size: LeucipaClusterSize,
                 job_flow_role: str,
                 bootstrap_scripts: Dict[str, str] = None,
                 master_instance_type: str = None,
                 core_instance_type: str = None,
                 task_instance_type: str = None):
        self.name = name
        self.dag = None  # set by a parent
        self.cluster_size = cluster_size
        self.job_flow_role = job_flow_role
        self.bootstrap_scripts = bootstrap_scripts
        self.master_instance_type = master_instance_type
        self.core_instance_type = core_instance_type
        self.task_instance_type = task_instance_type

    def get_job_flow_overrides(self) -> Dict:

        if self.master_instance_type is None:
            self.master_instance_type = "m5.xlarge"

        if self.core_instance_type is None:
            self.core_instance_type = "m5.xlarge"

        if self.task_instance_type is None:
            self.task_instance_type = "m5.xlarge"

        return {
            "Name": self.name + "-" + self.cluster_size.suffix,
            "ReleaseLabel": "emr-6.12.0",
            "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
            "BootstrapActions": self.build_bootstrap_actions_block(),
            "LogUri": f"s3n://{Variable.get(EMR_S3_BUCKET)}/emr_logs/",
            "Configurations": [
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                        }
                    ],
                }
            ],
            "Instances": {
                "InstanceGroups": [{
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": self.master_instance_type,
                    "InstanceCount": self.cluster_size.master_nodes
                }, {
                    "Name": "Core nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": self.core_instance_type,
                    "InstanceCount": self.cluster_size.core_nodes,
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "SizeInGB": 200,
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }
                        ]
                    }
                }, {
                    "Name": "Task nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "TASK",
                    "InstanceType": self.task_instance_type,
                    "InstanceCount": self.cluster_size.task_nodes,
                }],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
                "Ec2SubnetId": Variable.get(EMR_SUBNET_ID),
            },
            "JobFlowRole": self.job_flow_role,
            "ServiceRole": Variable.get(EMR_SERVICE_ROLE),
            "AutoTerminationPolicy": {
                "IdleTimeout": 3600
            },
        }

    def build_bootstrap_actions_block(self):
        if self.bootstrap_scripts is not None:
            bootstrap_scripts = self._merge_dicts(DEFAULT_BOOTSTRAP_SCRIPTS, self.bootstrap_scripts)
        else:
            bootstrap_scripts = DEFAULT_BOOTSTRAP_SCRIPTS
        result = []
        for name, path in bootstrap_scripts.items():
            s3_path = AssetUtil.adapt_to_s3(path)
            result.append({
                "Name": name,
                "ScriptBootstrapAction": {
                    "Path": s3_path,
                }
            })
        return result

    @staticmethod
    def _merge_lists(a: List, b: List) -> List:
        result = []
        for item in a + b:
            if item not in result:
                result.append(item)
        return result

    @staticmethod
    def _merge_dicts(a: Dict, b: Dict) -> Dict:
        result = a.copy()
        for key, value in b.items():
            result[key] = value
        return result
