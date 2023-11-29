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
            cluster_size=LeucipaClusterSizes.SMALL,
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
        ),
    )

    files_save_step = LeucipaEmrAddStepsOperator(
        task_id="plink_data_model_files_save",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.SMALL,
            leucipa_application="app_plink_data_model_files_save",
            leucipa_config={
                "soap_secret_arn":  Variable.get(PRODML_STAGING_SERVER_SECRET_ARN),
                "data_model_xml_item_s3_path": "s3://" + Variable.get(TENANT_XOM_US_LANDING) + "/SENSOR/PLINK_DATA_MODEL/{}/{}.xml",
            }
        ),
    )

    process_step = LeucipaEmrAddStepsOperator(
        task_id="plink_data_model_files_process",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.SMALL,
            leucipa_application="app_plink_data_model_files_process",
            leucipa_config={
                "data_model_xml_all_s3_path": "s3://" + Variable.get(TENANT_XOM_US_LANDING) + "/SENSOR/PLINK_DATA_MODEL/{}/",
                "data_model_hudi_s3_path": "s3://" + Variable.get(TENANT_XOM_US_RAW) + "/SENSOR/PLINK_DATA_MODEL/LATEST/",
                "hudi_mode": "Append",
                "hudi_options": {
                    "hoodie.table.name": "PLINK_DATA_MODEL",
                    "hoodie.datasource.write.operation": "upsert",
                    "hoodie.datasource.write.recordkey.field": "id",
                    "hoodie.datasource.write.precombine.field": "id",
                    "hoodie.datasource.write.partitionpath.field": "partition_path",
                    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                    "hoodie.upsert.shuffle.parallelism": 20,
                    "hoodie.insert.shuffle.parallelism": 20,
                    "hoodie.parquet.small.file.limit": 52428800,
                    "hoodie.parquet.max.file.size": 209715200
                }
            },
            spark_packages=[
                "org.apache.spark:spark-protobuf_2.12:3.4.0",
                "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0",
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
        >> files_save_step >> files_save_step.watcher() \
        >> process_step >> process_step.watcher() \
        >> emr_cluster.terminator()
