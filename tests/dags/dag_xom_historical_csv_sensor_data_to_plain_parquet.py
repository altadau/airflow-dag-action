import os
import sys

from airflow.models import Variable

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ['AWS_DEFAULT_REGION'] = Variable.get("AWS_REGION")
# ======================================

from leucipa.leucipa_dag import LeucipaDAG
from leucipa.operators.leucipa_emr_create_operator import LeucipaEmrCreateJobFlowOperator
from leucipa.operators.leucipa_spark_application_operator import LeucipaEmrAddStepsOperator
from leucipa.op_configs.leucipa_spark_config import LeucipaSparkConfig
from leucipa.op_configs.leucipa_cluster_config import LeucipaClusterConfig
from leucipa.op_configs.leucipa_cluster_size import LeucipaClusterSizes
from leucipa.op_configs.leucipa_spark_size import LeucipaSparkSizes
from leucipa.constants.airflow_variables import *

with LeucipaDAG(
        tags=["xom"],
) as dag:
    emr_cluster = LeucipaEmrCreateJobFlowOperator(
        task_id="emr_cluster",
        dag=dag,
        leucipa_cluster_config=LeucipaClusterConfig(
            name=dag.leucipa_run_name,
            cluster_size=LeucipaClusterSizes.LARGE,
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
        ),
    )

    spark_app = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.LARGE,
            leucipa_application="app_s3_csv_to_s3_parquet",
            leucipa_config={
                "base_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/SENSOR/HISTORICAL/CSV_2023-05-02/",
                "input_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/SENSOR/HISTORICAL/CSV_2023-05-02/*/*.csv",
                "input_schema_path": "assets://schemas/schema_xom_sensor_csv_plain.json",
                # "input_filter": "APINumber IN ('3001537272', '3001537309')",
                "output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/HISTORICAL/PARQUET_2023-05-02/",
                "output_coalesce": 64,
                "output_partition_by": "APINumber",
                "header": "false"
            },
        ),
    )

    emr_cluster \
        >> spark_app >> spark_app.watcher() \
        >> emr_cluster.terminator()
