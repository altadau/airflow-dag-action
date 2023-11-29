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
            spark_size=LeucipaSparkSizes.MEDIUM,
            leucipa_application="app_datagap_report",
            leucipa_config={
                "hudi_input_path": data_path,
                "primary_field": primary_field,
                "timestamp_field": timestamp_field,
                "timestamp_partitioning": "yyyy-MM-dd",
                "pandas_date_range_freq": "D",  # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
                "report_output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/REPORT/dag_xom_datagap_report/{task_name}/",
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
            },
        ),
    )


with LeucipaDAG(
        tags=["xom", "report"],
        schedule_interval="0 23 * * *",  # 23:00 every day
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

    report_failure = create_report_step(
        task_name="report_failure",
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/LIVE/FAILURE/",
        primary_field="API10",
        timestamp_field="FailureDate"
    )
    emr_cluster \
        >> report_failure >> report_failure.watcher() \
        >> emr_cluster.terminator()

    report_productiondata = create_report_step(
        task_name="report_productiondata",
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/LIVE/PRODUCTIONDATA/",
        primary_field="API10",
        timestamp_field="RecordDate"
    )
    emr_cluster \
        >> report_productiondata >> report_productiondata.watcher() \
        >> emr_cluster.terminator()

    report_sensordata = create_report_step(
        task_name="report_sensordata",
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/LIVE/SENSORDATA/",
        primary_field="APINumber",
        timestamp_field="RecordDateTimeUTC"
    )
    emr_cluster \
        >> report_sensordata >> report_sensordata.watcher() \
        >> emr_cluster.terminator()

    report_welltestdata = create_report_step(
        task_name="report_welltestdata",
        data_path=f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/LIVE/WELLTESTDATA/",
        primary_field="APINumber",
        timestamp_field="RecordDateTimeUTC"
    )
    emr_cluster \
        >> report_welltestdata >> report_welltestdata.watcher() \
        >> emr_cluster.terminator()
