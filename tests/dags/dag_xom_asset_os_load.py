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
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
        ),
    )

    EMR_S3_BUCKET = Variable.get("EMR_S3_BUCKET")
    OPEN_SEARCH_HOST = "https://{}".format(Variable.get("OPEN_SEARCH_HOST"))
    OPEN_SEARCH_PORT = Variable.get("OPEN_SEARCH_PORT")

    spark_app = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.SMALL,
            leucipa_application="app_asset_to_opensearch",
            leucipa_config={
                "pg_secret_url_connection": Variable.get(RDS_POSTGRES_XOM_US_SECRET_ARN),
                "opensearch_nodes": OPEN_SEARCH_HOST,
                "opensearch_port": OPEN_SEARCH_PORT,
                "config_filepath": "assets://config_os/os_loader_config"
            },
            spark_packages=[
                "org.opensearch.client:opensearch-hadoop:1.0.1",
                "org.postgresql:postgresql:42.2.5"
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
