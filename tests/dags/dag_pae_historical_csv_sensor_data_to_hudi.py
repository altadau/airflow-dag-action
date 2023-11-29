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
        tags=["pae"],
) as dag:
    emr_cluster = LeucipaEmrCreateJobFlowOperator(
        task_id="emr_cluster",
        dag=dag,
        leucipa_cluster_config=LeucipaClusterConfig(
            name=dag.leucipa_run_name,
            cluster_size=LeucipaClusterSizes.LARGE,
            job_flow_role=Variable.get(TENANT_PAE_ARG_EMR_JOBFLOW_ROLE),
        ),
    )

    spark_app = LeucipaEmrAddStepsOperator(
        task_id="spark_app",
        dag=dag,
        leucipa_cluster_step=emr_cluster,
        leucipa_spark_config=LeucipaSparkConfig(
            name=dag.leucipa_run_name,
            spark_size=LeucipaSparkSizes.LARGE,
            leucipa_application="app_s3_csv_to_s3_hudi_opts_sprt",
            leucipa_config={
                #
                "base_path": f"s3://{Variable.get(TENANT_PAE_ARG_LANDING)}/SENSOR/HISTORICAL/",
                "input_path": f"s3://{Variable.get(TENANT_PAE_ARG_LANDING)}/SENSOR/HISTORICAL/*.csv",
                "input_schema_path": "assets://schemas/schema_pae_sensor_csv_plain.json",
                "header": True,
                "delimiter": ";",
                "input_filter": "pozo IN ('PLM-964(d)','PLP-841','PCG-269','PCH-801','PLP-872','PCG-1283(d)','PCD-1052','PCD-1298(d)','PCG-1191','PCG-830','PCD-961','PCG-1218(d)','PCH-824','PCG-263','PP-811','PCG-1312','PJ-853','PCG-949','PCD-1054(d)','PLM-916(d)','PCG-197','PLM-859','PCG-1270(d)','PCG-1251','PJ-804','PCG-899','PCD-1296(d)','PLM-871','PLM-921','PLM-846','PCG-1293(d)','PCG-803','PLM-908','PEC-807','PET-857(d)','PCD-1293','PCG-144','PLM-911','PET-861(d)','PET-801','PLM-920','PLM-931','PCG-812','PCD-825','PM-810','PCD-1172')",
                "hudi_id_concat_fields": ["pozo", "timestamp"],
                "hudi_partition_primary_field": "pozo",
                "hudi_partition_datetime_field": "timestamp",
                "hudi_partition_datetime_pattern": "yyyy-MM-dd HH:mm",
                "hudi_output_path": f"s3://{Variable.get(TENANT_PAE_ARG_RAW)}/SENSOR/HISTORICAL/HUDI_2023-10-18/",
                "hudi_mode": "Append",
                "hudi_options": {
                    "hoodie.table.name": "PAE_SENSOR",
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
