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
            cluster_size=LeucipaClusterSizes.XL,
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
        ),
    )

    spark_app = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.XL,
            leucipa_application="app_s3_csv_to_s3_hudi",
            leucipa_config={
                #
                "base_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/SENSOR/HISTORICAL/CSV_2023-05-02/",
                "input_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/SENSOR/HISTORICAL/CSV_2023-05-02/*/*.csv",
                "input_schema_path": "assets://schemas/schema_xom_sensor_csv_typed.json",
                "header": "false",
                # "input_filter": "APINumber IN ('3001537272', '3001537309')",  # testing purposes
                #
                "hudi_id_concat_fields": ["APINumber", "Source", "Measurement", "RecordDateTimeUTC"],
                "hudi_partition_primary_field": "APINumber",
                "hudi_partition_datetime_field": "RecordDateTimeUTC",
                "hudi_partition_datetime_pattern": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                "hudi_output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/HISTORICAL/HUDI_2023-05-02/",
                "hudi_mode": "Append",
                "hudi_options": {
                    "hoodie.table.name": "XOM_SENSOR",
                    "hoodie.datasource.write.operation": "upsert",
                    "hoodie.datasource.write.recordkey.field": "id",
                    "hoodie.datasource.write.precombine.field": "id",
                    "hoodie.datasource.write.partitionpath.field": "partition_path",
                    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                    "hoodie.upsert.shuffle.parallelism": 20,
                    "hoodie.insert.shuffle.parallelism": 20,
                    "hoodie.parquet.small.file.limit": 52428800,
                    "hoodie.parquet.max.file.size": 209715200,
                },
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
            },
        ),
    )

    emr_cluster \
        >> spark_app >> spark_app.watcher() \
        >> emr_cluster.terminator()
