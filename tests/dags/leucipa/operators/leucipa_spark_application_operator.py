import os
import sys

from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================

from leucipa.utils.assert_util import AssertUtil
from leucipa.op_configs.leucipa_spark_config import LeucipaSparkConfig
from leucipa.toolbox.observability.emr_logs import pull_logs


class LeucipaEmrAddStepsOperator(EmrAddStepsOperator):
    """
    Provides defaults to create application steps -
    with the same arguments as in EmrAddStepsOperator(..), and with provided arguments always taking priority.

    I.e. it could be used as a EmrAddStepsOperator's drop-in replacement::

        # Just use `LeucipaSparkApplicationOperator` instead of `EmrAddStepsOperator`
        LeucipaSparkApplicationOperator(
            task_id="add_steps",
            steps=SPARK_STEPS,
            dag=dag,
            ...
        )

    Replacements::

        - `leucipa_cluster_step: LeucipaEmrAddStepsOperator` cannot be used together with `job_flow_id`
        - `leucipa_spark_config: LeucipaSparkConfig` cannot be used together with `steps`
    """

    def __init__(self, **kwargs):
        # created on-demand
        self._watcher = None

        # set job_flow_id
        if (kwargs.get("job_flow_id") is not None) and (kwargs.get("leucipa_cluster_step") is not None):
            raise ValueError("Cannot be defined both: `leucipa_cluster_step` and `job_flow_id`")
        if (kwargs.get("job_flow_id") is None) and (kwargs.get("leucipa_cluster_step") is not None):
            leucipa_cluster_step: EmrCreateJobFlowOperator = kwargs.get("leucipa_cluster_step")
            job_flow_task_id = leucipa_cluster_step.task_id
            job_flow_id = "{{ task_instance.xcom_pull(task_ids='" + job_flow_task_id + "', key='return_value') }}"
            kwargs["job_flow_id"] = job_flow_id

        # default for aws_conn_id
        if kwargs.get("aws_conn_id") is None:
            kwargs["aws_conn_id"] = "aws_default"

        # default for do_xcom_push
        if kwargs.get("do_xcom_push") is None:
            kwargs["do_xcom_push"] = True

        # define steps
        leucipa_spark_config = kwargs.get("leucipa_spark_config")
        if (kwargs.get("steps") is not None) and (leucipa_spark_config is not None):
            raise ValueError("Cannot be defined both: `leucipa_spark_config` and `steps`")
        if (kwargs.get("steps") is None) and (leucipa_spark_config is not None):
            if isinstance(leucipa_spark_config, LeucipaSparkConfig):
                leucipa_spark_config.dag = kwargs.get("dag")
                kwargs["steps"] = leucipa_spark_config.get_steps()
            else:
                raise ValueError("Expected `leucipa_spark_config` to be of type `LeucipaSparkConfig`.")

        # kwargs cleanup
        keys_to_remove = [key for key in kwargs if key.startswith("leucipa_")]
        for key in keys_to_remove:
            kwargs.pop(key)

        super().__init__(**kwargs)

    def watcher(self) -> EmrStepSensor:
        if self._watcher is not None:
            return self._watcher

        AssertUtil.not_none(self.steps, "Should not be None: `steps`")
        AssertUtil.not_none(self.task_id, "Should not be None: `task_id`")
        AssertUtil.not_none(self.job_flow_id, "Should not be None: `job_flow_id`")
        AssertUtil.not_none(self.aws_conn_id, "Should not be None: `aws_conn_id`")
        AssertUtil.not_none(self.dag, "Should not be None: `dag`")

        last_cluster_step_pos = len(self.steps) - 1
        self._watcher = EmrStepSensor(
            task_id=f"{self.task_id}_watcher",
            job_flow_id=self.job_flow_id,
            step_id="{{ task_instance.xcom_pull(task_ids='" + self.task_id + "', key='return_value')"
                    + "[" + str(last_cluster_step_pos) + "] }}",
            aws_conn_id=self.aws_conn_id,
            dag=self.dag,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            on_failure_callback=pull_logs
        )

        return self._watcher
