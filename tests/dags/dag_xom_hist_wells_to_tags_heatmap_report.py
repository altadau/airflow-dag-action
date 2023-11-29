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


def create_report_step(task_name, input_path, x_field, y_field):
    return LeucipaEmrAddStepsOperator(
        task_id=task_name,
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=task_name,
            spark_size=LeucipaSparkSizes.LARGE,
            leucipa_application="app_heatmap_report",
            leucipa_config={
                "input_path": input_path,
                "y_field": y_field,
                "x_field": x_field,
                "report_output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/REPORT/dag_xom_hist_wells_to_tags_heatmap_report/{task_name}/",
            },
        ),
    )


with LeucipaDAG(
        tags=["xom", "report"],
) as dag:
    emr_cluster = LeucipaEmrCreateJobFlowOperator(
        task_id="emr_cluster",
        dag=dag,
        leucipa_cluster_config=LeucipaClusterConfig(
            name=dag.leucipa_run_name,
            cluster_size=LeucipaClusterSizes.LARGE,
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
            bootstrap_scripts={
                "Plot libs": "assets://bootstrap_actions/dependencies_plots.sh"
            },
        ),
    )

    report_sensor_hist = create_report_step(
        task_name="report_sensor_hist",
        input_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/VERSION_10/",
        x_field="Measurement",
        y_field="APINumber"
    )
    emr_cluster \
        >> report_sensor_hist >> report_sensor_hist.watcher() \
        >> emr_cluster.terminator()

    report_production_hist = create_report_step(
        task_name="report_production_hist",
        input_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/PRODUCTION/VERSION_10/",
        x_field="measurement",
        y_field="API10",
    )
    emr_cluster \
        >> report_production_hist >> report_production_hist.watcher() \
        >> emr_cluster.terminator()

    report_sensor_live = create_report_step(
        task_name="report_sensor_live",
        input_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/LIVE/SENSORDATA/",
        x_field="Measurement",
        y_field="APINumber"
    )
    emr_cluster \
        >> report_sensor_live >> report_sensor_live.watcher() \
        >> emr_cluster.terminator()
