import logging
import os
from datetime import datetime

import boto3
from airflow.models import Variable
from botocore.exceptions import ClientError

AIRFLOW_S3_BUCKET = Variable.get("AIRFLOW_BUCKET")


class LoggingS3Handler:
    LOG_FILE_NAME = f"{datetime.utcnow().isoformat()}.log"
    IS_INIT = False

    @staticmethod
    def init() -> None:
        # logging.basicConfig(filename=LoggingS3Handler.LOG_FILE_NAME, level=logging.INFO)
        logging.info("test")
        LoggingS3Handler.IS_INIT = True

    @staticmethod
    def upload_to_3s(prefix: str = None) -> None:
        if LoggingS3Handler.IS_INIT is False:
            raise Exception("`LoggingS3Handler.upload_to_3s(..)` should be called after `LoggingS3Handler.init(..)`")
        s3 = boto3.client('s3')
        local_file = LoggingS3Handler.LOG_FILE_NAME
        if prefix is None:
            s3_path = f"s3://{AIRFLOW_S3_BUCKET}/logs/{local_file}"
        else:
            s3_path = f"s3://{AIRFLOW_S3_BUCKET}/logs/{prefix}/{local_file}"
        try:
            bucket_name, file_key = s3_path.replace("s3://", "").split("/", 1)
            s3.upload_file(local_file, bucket_name, file_key)
            os.remove(local_file)
        except FileNotFoundError as e:
            raise Exception(f"{e}")
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise Exception(f"Cannot upload file to '{s3_path}', access denied. Original exception: {e}")
            else:
                raise Exception(f"{e}")
