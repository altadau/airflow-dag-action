import sys
import os
import json
from datetime import datetime, timezone
from typing import Dict, List

import boto3
import yaml
from botocore.exceptions import ClientError, NoCredentialsError

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================

from leucipa.utils.asset_util import AssetUtil


class S3Util:

    @staticmethod
    def read_as_bytes(s3_path: str) -> bytes:
        s3_path = AssetUtil.adapt_to_s3(s3_path)
        s3 = boto3.client('s3')
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        try:
            s3_obj = s3.get_object(Bucket=bucket, Key=key)
            return s3_obj["Body"].read()
        except (s3.exceptions.NoSuchBucket, s3.exceptions.NoSuchKey) as e:
            raise Exception(f"Cannot read file '{s3_path}', S3 path does not exist. Original exception: {e}")
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise Exception(f"Cannot read file '{s3_path}', access denied. Original exception: {e}")
        except NoCredentialsError as e:
            raise Exception(f"Cannot write file '{s3_path}', unable to locate credentials. Original exception: {e}")

    @staticmethod
    def read_as_text(s3_path: str) -> str:
        return S3Util.read_as_bytes(s3_path).decode("utf-8")

    @staticmethod
    def read_as_dict(s3_path: str) -> Dict:
        s3_path = AssetUtil.adapt_to_s3(s3_path)
        content = S3Util.read_as_text(s3_path)
        if s3_path.endswith('.json'):
            return json.loads(content)
        elif s3_path.endswith('.yml') or s3_path.endswith('.yaml'):
            return yaml.safe_load(content)
        else:
            raise ValueError(f"Cannot read `{s3_path}` into dictionary, available file types: *.json, *.yml, *yaml")

    @staticmethod
    def write(s3_path: str, body: any) -> None:
        s3 = boto3.client('s3')
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        try:
            s3.put_object(Body=body, Bucket=bucket, Key=key)
        except (s3.exceptions.NoSuchBucket, s3.exceptions.NoSuchKey) as e:
            raise Exception(f"Cannot write file '{s3_path}', S3 path does not exist. Original exception: {e}")
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise Exception(f"Cannot write file '{s3_path}', access denied. Original exception: {e}")
        except NoCredentialsError as e:
            raise Exception(f"Cannot write file '{s3_path}', unable to locate credentials. Original exception: {e}")

    @staticmethod
    def list(s3_path: str) -> List[str]:
        s3_path = AssetUtil.adapt_to_s3(s3_path)
        result = []
        s3 = boto3.client('s3')
        bucket_name, folder_path = s3_path.replace("s3://", "").split("/", 1)
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
            if "Contents" in response:
                for key in [item['Key'] for item in response['Contents']]:
                    result.append(f"s3://{bucket_name}/{key}")
            else:
                raise Exception(f"S3 path not found: `{s3_path}`")
        except (s3.exceptions.NoSuchBucket, s3.exceptions.NoSuchKey) as e:
            raise Exception(f"Cannot read file '{s3_path}', S3 path does not exist. Original exception: {e}")
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise Exception(f"Cannot read file '{s3_path}', access denied. Original exception: {e}")
            else:
                raise Exception(e)
        except NoCredentialsError as e:
            raise Exception(f"Cannot write file '{s3_path}', unable to locate credentials. Original exception: {e}")
        return result

    @staticmethod
    def list_older_than(s3_path: str, cutoff_timestamp: int) -> List[str]:
        s3_path = AssetUtil.adapt_to_s3(s3_path)
        result = []
        s3 = boto3.client('s3')
        bucket_name, prefix = s3_path.replace("s3://", "").split("/", 1)
        try:
            cutoff_date = datetime.fromtimestamp(cutoff_timestamp, timezone.utc)
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['LastModified'] > cutoff_date:
                            key = obj['Key']
                            result.append(f"s3://{bucket_name}/{key}")
        except (s3.exceptions.NoSuchBucket, s3.exceptions.NoSuchKey) as e:
            raise Exception(f"Cannot read file '{s3_path}', S3 path does not exist. Original exception: {e}")
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                raise Exception(f"Cannot read file '{s3_path}', access denied. Original exception: {e}")
            else:
                raise Exception(e)
        except NoCredentialsError as e:
            raise Exception(f"Cannot write file '{s3_path}', unable to locate credentials. Original exception: {e}")
        return result
