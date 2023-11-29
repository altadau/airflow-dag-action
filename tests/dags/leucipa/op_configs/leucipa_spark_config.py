import base64
import os
import sys
from typing import List, Dict

import yaml
import json

from airflow.models import Variable

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================

from leucipa.utils.s3_config_scanner import S3ConfigScanner
from leucipa.utils.asset_util import AssetUtil
from leucipa.op_configs.leucipa_spark_size import LeucipaSparkSize
from leucipa.constants.airflow_variables import *

# default spark script that proxies applications
LEUCIPA_SPARK_SCRIPT = "assets://spark_scripts/leucipa_spark_script.py"


class LeucipaSparkConfig:
    """
    Spark environment.

    Note::

        - `leucipa_application` cannot be used together with `spark_script` (even in autoconfig)
        - `leucipa_config` cannot be used together with `spark_params` (even in autoconfig)
        - `autoconfig` cannot be used together with ANY of `spark_*` or `leucipa_*` arguments
           (since `autoconfig` dictionary (constructed from yaml file) defines these arguments on the top level)
    """

    def __init__(self, name: str,
                 spark_size: LeucipaSparkSize,
                 spark_script: str = None,
                 spark_params: Dict = None,
                 spark_packages: List[str] = None,
                 spark_conf: Dict[str, str] = None,
                 leucipa_application: str = None,
                 leucipa_config: Dict = None,
                 autoconfig: str = None):
        self.name = name
        self.dag = None  # set by a parent
        self.spark_size = spark_size

        if autoconfig is not None:
            # autoconfig has `spark_*` and `leucipa_*` arguments within itself,
            # so we are raising exception if any of those are defined - to avoid confusion
            if self._any_not_none(spark_script, spark_params, spark_packages, spark_conf,
                                  leucipa_application, leucipa_config):
                raise ValueError("`autoconfig` cannot be used together with ANY of `spark_*` or `leucipa_*` arguments")
            # reading actual dictionary value for autoconfig
            autoconfig_dict = S3ConfigScanner.get_config_dict(autoconfig)
            # and then overwriting all those dependant arguments
            self.spark_script = autoconfig_dict.get("spark_script")
            self.spark_params = autoconfig_dict.get("spark_params")
            self.spark_packages = autoconfig_dict.get("spark_packages")
            self.spark_conf = autoconfig_dict.get("spark_conf")
            self.leucipa_application = autoconfig_dict.get("leucipa_application")
            self.leucipa_config = autoconfig_dict.get("leucipa_config")
        else:
            # not-autoconfig case
            self.spark_script = spark_script
            self.spark_params = spark_params
            self.spark_packages = spark_packages
            self.spark_conf = spark_conf
            self.leucipa_application = leucipa_application
            self.leucipa_config = leucipa_config

        ####
        # Basically we can use either of two options:
        # - spark_script and spark_params(optional)
        # - OR leucipa_application and leucipa_config(optional)
        ####
        if (self.spark_script is None) and (self.leucipa_application is None):
            raise ValueError("Both cannot be None: `spark_script` and `leucipa_application`")
        if (self.spark_script is not None) and (self.leucipa_application is not None):
            raise ValueError("`spark_script` cannot be used together with `leucipa_application`")
        if (self.spark_script is not None) and (self.leucipa_config is not None):
            raise ValueError("`spark_script` cannot be used together with `leucipa_config`")
        if (self.leucipa_application is not None) and (self.spark_params is not None):
            raise ValueError("`leucipa_application` cannot be used together with `spark_params`")

        # define all spark packages (default + specified)
        if self.spark_packages is not None:
            self.all_spark_packages = self._merge_lists(self._get_default_spark_packages(), self.spark_packages)
        else:
            self.all_spark_packages = self._get_default_spark_packages()

        # define all spark conf values (default + specified)
        if self.spark_conf is not None:
            self.all_spark_conf = self._merge_dicts(spark_size.get_spark_conf(), self.spark_conf)
        else:
            self.all_spark_conf = spark_size.get_spark_conf()

    def get_steps(self) -> List:

        # define Spark `--packages` argument
        packages_str = ','.join(self.all_spark_packages)
        packages_args = ['--packages', packages_str]

        # define Spark `--conf` arguments
        conf_args = []
        for conf_key, conf_value in self.all_spark_conf.items():
            conf_args = conf_args + ["--conf", f"{conf_key}={conf_value}"]

        # locate spark script (S3 path)
        if self.leucipa_application is not None:
            spark_script_s3_path = AssetUtil.adapt_to_s3(LEUCIPA_SPARK_SCRIPT)
        else:
            spark_script_s3_path = AssetUtil.adapt_to_s3(self.spark_script)

        # define Spark parameter arguments
        params_args = self._define_spark_params_args()

        return [
            {
                "Name": self.name + "-" + self.spark_size.suffix,
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                                "spark-submit",
                                "--deploy-mode", "client",

                                # "--packages", "..."
                            ] + packages_args + [

                                # "--conf", "...",
                                # "--conf", "...",
                            ] + conf_args + [

                                "--py-files", f"s3://{Variable.get(EMR_S3_BUCKET)}/{Variable.get(ENV_TYPE_PREFIX)}-leucipa-data-ingest/src.zip",

                                # s3://.../script-name.py
                                spark_script_s3_path,

                                # "--param_1", "abc",
                                # "--param_2", "2",
                            ] + params_args + [

                                #
                            ],
                },
            }
        ]

    def _define_spark_params_args(self):
        params_args = []
        if self.leucipa_application is not None:

            leucipa_extras = {}
            leucipa_extras["assets_s3_path"] = f"s3://{Variable.get(EMR_S3_BUCKET)}/{Variable.get(ENV_TYPE_PREFIX)}-leucipa-data-ingest/aux/assets"
            leucipa_extras["run_timestamp"] = "{{ execution_date.timestamp() | int }}"

            params_dict = {
                "leucipa_application": self.leucipa_application,
                "leucipa_config": self.leucipa_config,
                "leucipa_extras": leucipa_extras,
                "spark_packages": self.all_spark_packages,
                "spark_conf": self.all_spark_conf,
            }

            params_json = json.dumps(params_dict)
            params_args = [
                "--params_json", params_json,
            ]
        elif self.spark_params is not None:
            for param_key, param_value in self.spark_params.items():
                params_args = params_args + [f"--{param_key}", f"{param_value}"]
        return params_args

    @staticmethod
    def _get_default_spark_packages() -> List:
        return [
            "org.apache.hadoop:hadoop-aws:3.3.3",
            "com.amazonaws:aws-java-sdk-bundle:1.12.490"
        ]

    @staticmethod
    def _merge_lists(a: List, b: List) -> List:
        result = []
        for item in a + b:
            if item not in result:
                result.append(item)
        return result

    @staticmethod
    def _merge_dicts(a: Dict, b: Dict) -> Dict:
        result = a.copy()
        for key, value in b.items():
            result[key] = value
        return result

    def _any_not_none(self, *args):
        return any(arg is not None for arg in args)
