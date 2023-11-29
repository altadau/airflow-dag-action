import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# DEFAULTS
AWS_REGION = Variable.get("AWS_REGION")
os.environ['AWS_DEFAULT_REGION'] = Variable.get("AWS_REGION")
EMR_S3_BUCKET = Variable.get("EMR_S3_BUCKET")
ENV_TYPE_PREFIX = Variable.get("ENV_TYPE_PREFIX")
py_files_zip = f"s3://{EMR_S3_BUCKET}/{ENV_TYPE_PREFIX}-leucipa-data-ingest/src.zip"


# OVERWRITE
dag_name = "dag_example_plain_api"
run_name = f"{dag_name}-{datetime.now().strftime('%s')}"
job_flow_role = Variable.get("TENANT_XOM_US_EMR_JOBFLOW_ROLE")
ENV_TYPE_PREFIX = Variable.get("ENV_TYPE_PREFIX")
spark_script_path = f"s3://{EMR_S3_BUCKET}/{ENV_TYPE_PREFIX}-leucipa-data-ingest/aux/assets/spark_scripts/spark_data_ingest_example.py"

create_cluster = {
    "Name": run_name,
    "ReleaseLabel": "emr-6.11.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "BootstrapActions": [
        {
            "Name": "Dependencies",
            "ScriptBootstrapAction": {
                "Path": f"s3://{EMR_S3_BUCKET}/{ENV_TYPE_PREFIX}-leucipa-data-ingest/aux/assets/bootstrap_actions/dependencies.sh",
            },
        }
    ],
    "LogUri": f"s3n://{EMR_S3_BUCKET}/emr_logs/",
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [{
            "Name": "Master nodes",
            "Market": "ON_DEMAND",
            "InstanceRole": "MASTER",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 1
        }, {
            "Name": "Core nodes",
            "Market": "ON_DEMAND",
            "InstanceRole": "CORE",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 2,
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [
                    {
                        "VolumeSpecification": {
                            "SizeInGB": 10,
                            "VolumeType": "gp2"
                        },
                        "VolumesPerInstance": 1
                    }
                ]
            }
        }, {
            "Name": "Task nodes",
            "Market": "ON_DEMAND",
            "InstanceRole": "TASK",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 2,
        }],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": Variable.get("EMR_SUBNET_ID"),
    },
    "JobFlowRole": job_flow_role,
    "ServiceRole": Variable.get("EMR_SERVICE_ROLE"),
    "AutoTerminationPolicy": {
        "IdleTimeout": 3600
    },
}

spark_submit_command = [
    "spark-submit",
    "--deploy-mode", "client",
    "--packages", "com.amazonaws:aws-java-sdk-bundle:1.12.31,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1",
    "--conf", "spark.driver.memory=4g",
    "--conf", "spark.executor.memory=10g",
    "--conf", "spark.executor.cores=1",
    "--conf", "spark.executor.instances=15",
    "--conf", "spark.default.parallelism=48",
    "--conf", "spark.yarn.executor.memoryOverhead=1g",
    "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
    "--conf", "spark.kryoserializer.buffer.max=256m",
    "--conf", "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    "--py-files", py_files_zip,
    spark_script_path,
    "--config_value_str", "abc",
    "--config_value_num", "2"
]

cluster_steps = [
    {
        "Name": run_name,
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": spark_submit_command,
        },
    }
]

with DAG(
        dag_name,
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": datetime(2020, 10, 17),
            "email": ["airflow@airflow.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        max_active_runs=1,
        schedule_interval=None,
        catchup=False,
        tags=["leucipa-data-ingest", "example", "AUTO_GIT_RELEASE_VERSION"],
) as dag:
    start_data_pipeline = EmptyOperator(task_id="start_data_pipeline", dag=dag)
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=create_cluster,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        dag=dag,
        region_name=AWS_REGION,
    )
    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=cluster_steps,
        do_xcom_push=True,
        dag=dag,
    )
    last_step = len(cluster_steps) - 1
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" + str(
            last_step) + "] }}",
        aws_conn_id="aws_default",
        dag=dag,
    )
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        dag=dag,
    )
    end_data_pipeline = EmptyOperator(task_id="end_data_pipeline", dag=dag)

    start_data_pipeline >> create_emr_cluster
    create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
    terminate_emr_cluster >> end_data_pipeline
