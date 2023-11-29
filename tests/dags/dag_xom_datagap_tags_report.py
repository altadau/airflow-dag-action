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


def create_report_step(task_name, data_path, primary_field, timestamp_field):
    return LeucipaEmrAddStepsOperator(
        task_id=task_name,
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=task_name,
            spark_size=LeucipaSparkSizes.LARGE,
            leucipa_application="app_datagap_report",
            leucipa_config={
                "hudi_input_path": data_path,
                "primary_field": primary_field,
                "timestamp_field": timestamp_field,
                "timestamp_partitioning": "yyyy-MM",
                "pandas_date_range_freq": "MS",  # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
                "report_output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/REPORT/dag_xom_datagap_tags_report/{task_name}/",
            },
            spark_packages=[
                "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0",
            ],
            spark_conf={
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryoserializer.buffer.max": "256m",
                "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
                "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
                "spark.sql.legacy.timeParserPolicy": "LEGACY",
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
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/VERSION_10/",
        primary_field="Measurement",
        timestamp_field="RecordDateTimeUTC"
    )
    emr_cluster \
        >> report_sensor_hist >> report_sensor_hist.watcher() \
        >> emr_cluster.terminator()

    report_production_hist = create_report_step(
        task_name="report_production_hist",
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/PRODUCTION/VERSION_10/",
        primary_field="measurement",
        timestamp_field="recorddateutc"
    )
    emr_cluster \
        >> report_production_hist >> report_production_hist.watcher() \
        >> emr_cluster.terminator()

    report_sensor_live = create_report_step(
        task_name="report_sensor_live",
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/LIVE/SENSORDATA/",
        primary_field="Measurement",
        timestamp_field="RecordDateTimeUTC"
    )
    emr_cluster \
        >> report_sensor_live >> report_sensor_live.watcher() \
        >> emr_cluster.terminator()
