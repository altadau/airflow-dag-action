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
            cluster_size=LeucipaClusterSizes.MEDIUM,
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
            bootstrap_scripts={
                # todo! - fixit - this script is env-specific:
                "Copy XOM PL event desc file": "assets://bootstrap_actions/cp_xom_plink_event.aa-dev.sh"
            }
        ),
    )

    plink_event_files_process = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.MEDIUM,
            leucipa_application="app_xom_plink_event_files_process",
            leucipa_config={
                "input_s3_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/topics/pdh-tsd/",
                "local_desc_path": "/mnt/tmp/xom_plink_event.desc",  # see `cp_xom_plink_event.aa-dev.sh` above
                "hudi_output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/XOM_PLINK_EVENT/TEST_2023-10-11_19-30/RESULT/",
                "hudi_mode": "Append",
                "hudi_options": {
                    "hoodie.table.name": "PLINK_KAFKA_EVENT",
                    "hoodie.datasource.write.operation": "upsert",
                    "hoodie.datasource.write.recordkey.field": "id",
                    "hoodie.datasource.write.precombine.field": "id",
                    "hoodie.datasource.write.partitionpath.field": "partition_path",
                    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                    "hoodie.upsert.shuffle.parallelism": 20,
                    "hoodie.insert.shuffle.parallelism": 20,
                    "hoodie.parquet.small.file.limit": 52428800,
                    "hoodie.parquet.max.file.size": 209715200,
                }
            },
            spark_packages=[
                "org.apache.spark:spark-protobuf_2.12:3.4.0",
                "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0"
            ],
            spark_conf={
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryoserializer.buffer.max": "256m",
                "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
                "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
            }
        ),
    )

    emr_cluster \
        >> plink_event_files_process >> plink_event_files_process.watcher() \
        >> emr_cluster.terminator()
