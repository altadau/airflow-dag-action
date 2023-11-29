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
from leucipa.op_configs.leucipa_cluster_config import LeucipaClusterConfig
from leucipa.op_configs.leucipa_cluster_size import LeucipaClusterSizes
from leucipa.op_configs.leucipa_spark_config import LeucipaSparkConfig
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
            cluster_size=LeucipaClusterSizes.SMALL,
            job_flow_role=Variable.get(TENANT_AGGREGATED_EMR_JOBFLOW_ROLE),
        ),
    )

    spark_app = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.SMALL,
            leucipa_application="app_tag_tables_import",
            leucipa_config={
                "pg_secret_url_connection": Variable.get(RDS_POSTGRES_DATA_ONBOARDING_DB_DATA_ONBOARDING_DB_SECRET_ARN),
                "input_paths": {
                    "tag_property": f"s3://{Variable.get(TENANT_AGGREGATED_LANDING)}/XLSX_FILES/Tag_Property_and_Units_V2.xlsx",
                    "tag_master": f"s3://{Variable.get(TENANT_AGGREGATED_LANDING)}/XLSX_FILES/Master_Tags.xlsx",
                    "tag_customer": f"s3://{Variable.get(TENANT_AGGREGATED_LANDING)}/XLSX_FILES/XOM_Customer_Well_tags.xlsx",
                }
            },
            spark_packages=[
                "com.crealytics:spark-excel_2.12:3.3.1_0.18.7",
            ],
            spark_conf={
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryoserializer.buffer.max": "128m",
            },
        ),
    )

    emr_cluster \
        >> spark_app >> spark_app.watcher() \
        >> emr_cluster.terminator()
