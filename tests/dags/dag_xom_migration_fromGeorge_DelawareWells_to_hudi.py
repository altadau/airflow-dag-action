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
from leucipa.op_configs.leucipa_spark_size import LeucipaSparkSizes, LeucipaSparkSize
from leucipa.constants.airflow_variables import *

with LeucipaDAG(
        tags=["xom", "migration"],
) as dag:
    emr_cluster = LeucipaEmrCreateJobFlowOperator(
        task_id="emr_cluster",
        dag=dag,
        leucipa_cluster_config=LeucipaClusterConfig(
            name=dag.leucipa_run_name,
            cluster_size=LeucipaClusterSizes.XL,
            job_flow_role=Variable.get(TENANT_XOM_US_EMR_JOBFLOW_ROLE),
            master_instance_type="m5.4xlarge",
        ),
    )

    spark_app = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSize(suffix="XL", master_nodes=1, core_nodes=8, task_nodes=32, driver_memory="32g"),
            leucipa_application="app_s3_csv_to_s3_hudi",
            leucipa_config={
                #
                "base_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/migration/2023-10-19T21-00/XOM/ESP_DATA/RAW_DONE/fromGeorge/DelawareWells/",
                "input_path": f"s3://{Variable.get(TENANT_XOM_US_LANDING)}/migration/2023-10-19T21-00/XOM/ESP_DATA/RAW_DONE/fromGeorge/DelawareWells/*/*.csv",
                "input_schema_path": "assets://schemas/schema_xom_sensor_csv_typed.json",
                "header": "false",
                # "input_filter": "APINumber IN ('3001537272', '3001537309')",  # testing purposes
                #
                "hudi_id_concat_fields": ["APINumber", "Source", "Measurement", "RecordDateTimeUTC"],
                "hudi_partition_primary_field": "APINumber",
                "hudi_partition_datetime_field": "RecordDateTimeUTC",
                "hudi_partition_datetime_pattern": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                "hudi_output_path": f"s3://{Variable.get(TENANT_XOM_US_RAW)}/SENSOR/VERSION_14/",
                "hudi_mode": "Append",
                "hudi_options": {
                    "hoodie.table.name": "XOM_SENSOR",
                    "hoodie.datasource.write.operation": "insert",
                    "hoodie.datasource.insert.dup.policy": "drop",
                    "hoodie.datasource.write.recordkey.field": "id",
                    "hoodie.datasource.write.precombine.field": "id",
                    "hoodie.datasource.write.partitionpath.field": "partition_path",
                    "hoodie.cleaner.commits.retained": 0,
                    "hoodie.parquet.compression.codec": "snappy",
                    "hoodie.metadata.enable": "false",
                },
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
                "spark.sql.files.openCostInBytes": 40485760,
                "spark.sql.files.maxPartitionBytes": 568435456,
            },
        ),
    )

    emr_cluster \
        >> spark_app >> spark_app.watcher() \
        >> emr_cluster.terminator()
