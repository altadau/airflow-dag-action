import os
import sys

import yaml
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ['AWS_DEFAULT_REGION'] = Variable.get("AWS_REGION")
# ======================================

from leucipa.leucipa_dag import LeucipaDAG
from leucipa.utils.s3_config_scanner import S3ConfigScanner
from leucipa.operators.leucipa_emr_create_operator import LeucipaEmrCreateJobFlowOperator
from leucipa.operators.leucipa_spark_application_operator import LeucipaEmrAddStepsOperator
from leucipa.op_configs.leucipa_cluster_config import LeucipaClusterConfig
from leucipa.op_configs.leucipa_cluster_size import LeucipaClusterSize

"""
    Run multiple `s3-dist-cp` commands (in-parallel) on created EMR cluster.
    Edit config at: `assets://configs/config_s3_dist_cp.yaml`
"""

with LeucipaDAG(
        tags=["xom", "migration"]
) as dag:

    config = S3ConfigScanner.get_config_dict(
        path="assets://configs/config_s3_dist_cp.yaml"
    )
    config_str = yaml.safe_dump(config, default_flow_style=False)

    emr_cluster = LeucipaEmrCreateJobFlowOperator(
        task_id="emr_cluster",
        dag=dag,
        leucipa_cluster_config=LeucipaClusterConfig(
            name=dag.leucipa_run_name,
            cluster_size=LeucipaClusterSize(suffix="CUSTOM",
                                            master_nodes=1,
                                            core_nodes=int(config["core_nodes"]),
                                            task_nodes=int(config["task_nodes"])),
            job_flow_role=Variable.get(config["job_flow_role"]),
        ),
    )

    description_step = PythonOperator(
        task_id="description",
        python_callable=print,
        op_args=[config_str],
    )
    description_step >> emr_cluster

    for item in config["s3_dist_cp"]:
        cp_step = LeucipaEmrAddStepsOperator(
            task_id=item["name"],
            dag=dag,
            leucipa_cluster_step=emr_cluster,
            steps=[{
                "Name": dag.leucipa_run_name + "-" + item["name"],
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "s3-dist-cp",
                        "--src=" + item["src"].replace("s3://", "s3a://"),
                        "--dest=" + item["dest"].replace("s3://", "s3a://"),
                        ],
                },
            }],
        )
        emr_cluster >> cp_step >> cp_step.watcher() >> emr_cluster.terminator()
