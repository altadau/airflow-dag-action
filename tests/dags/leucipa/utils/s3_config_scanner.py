import json
import os
import sys
from typing import List, Dict

import boto3
import yaml
from jinja2 import StrictUndefined, Template

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# ======================================

from leucipa.utils.airflow_vars_as_dict import AirflowVarsAsDict
from leucipa.utils.asset_util import AssetUtil


class S3ConfigScanner:
    """
    Read S3 path and return dictionary of configs where keys are S3 paths and values are dict.
    Supports custom Jinja2 variable substitutions.

    Example usage ::

        S3ConfigScanner.get_configs_dict("s3://bucket-name/config-path")

    Result ::

        {
            "s3://bucket-name/config-path/config_ab.yaml": {"config_key_1": "a", "config_key_2": "b"},
            "s3://bucket-name/config-path/config_cd.yaml": {"config_key_1": "c", "config_key_2": "d"}
        }
        
    Available substitutions in configs ::

        {{ airflow.NAME_OF_AIRFLOW_VARIABLE }}
    """

    _s3_client = boto3.client("s3")

    @staticmethod
    def get_configs_dict(s3_configs_path: str) -> Dict[str, Dict]:
        result = {}
        for s3_config_path in S3ConfigScanner._list_s3_config_files(s3_configs_path):
            try:
                result[s3_config_path] = S3ConfigScanner.get_config_dict(s3_config_path)
            except Exception as e:
                # # TODO resolve
                # logging.error(e)
                raise e
        return result

    @staticmethod
    def _list_s3_config_files(s3_path: str) -> List[str]:
        bucket_name, folder_path = s3_path.replace("s3://", "").split("/", 1)
        response = S3ConfigScanner._s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        if "Contents" in response:
            # Filter for *json or *.yml or *.yaml files
            filtered_keys = [item['Key'] for item in response['Contents']
                             if item['Key'].endswith('.json')
                             or item['Key'].endswith('.yml')
                             or item['Key'].endswith('.yaml')]
            for key in filtered_keys:
                yield f"s3://{bucket_name}/{key}"
        else:
            raise Exception(f"S3 path not found: `{s3_path}`")

    @staticmethod
    def get_config_dict(path, params: Dict[str, str] = None):
        s3_path = AssetUtil.adapt_to_s3(path)
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        s3_obj = S3ConfigScanner._s3_client.get_object(Bucket=bucket, Key=key)
        content = s3_obj["Body"].read().decode("utf-8")
        rendered_content = S3ConfigScanner._render_as_template(content, params)
        if key.endswith('.json'):
            return json.loads(rendered_content)
        elif key.endswith('.yml') or key.endswith('.yaml'):
            return yaml.safe_load(rendered_content)

    @staticmethod
    def _render_as_template(content: str, params: Dict[str, str] = None) -> str:
        template = Template(content, undefined=StrictUndefined)
        # see `AirflowVarsAsDict` for more details
        if params is None:
            params = {}
        return template.render(airflow=AirflowVarsAsDict(), params=params)
