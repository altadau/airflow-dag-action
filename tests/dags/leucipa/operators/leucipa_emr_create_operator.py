import os
import sys

from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.utils.trigger_rule import TriggerRule

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================

from leucipa.utils.assert_util import AssertUtil


class LeucipaEmrCreateJobFlowOperator(EmrCreateJobFlowOperator):
    """
    Provides defaults to create Leucipa EMR clusters -
    with the same arguments as in EmrCreateJobFlowOperator(..), and with provided arguments always taking priority.

    I.e. it could be used as a EmrCreateJobFlowOperator's drop-in replacement::

        # Just use `LeucipaEmrCreateJobFlowOperator` instead of `EmrCreateJobFlowOperator`
        LeucipaEmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            job_flow_overrides=JOB_FLOW_OVERRIDES,
            dag=dag,
            ...
        )

    Replacements::

        - `leucipa_cluster_config: LeucipaClusterConfig` cannot be used together with `job_flow_overrides`
    """

    def __init__(self, **kwargs):
        # created on-demand
        self._terminator = None

        # define `job_flow_overrides` from `leucipa_cluster_config`
        leucipa_cluster_config = kwargs.get("leucipa_cluster_config")
        if (kwargs.get("job_flow_overrides") is not None) and (leucipa_cluster_config is not None):
            raise ValueError("Cannot be defined both: `leucipa_cluster_config` and `job_flow_overrides`")
        if (kwargs.get("job_flow_overrides") is None) and (leucipa_cluster_config is not None):
            leucipa_cluster_config.dag = kwargs.get("dag")
            kwargs["job_flow_overrides"] = leucipa_cluster_config.get_job_flow_overrides()

        # default for aws_conn_id
        if kwargs.get("aws_conn_id") is None:
            kwargs["aws_conn_id"] = "aws_default"

        # default for emr_conn_id
        if kwargs.get("emr_conn_id") is None:
            kwargs["emr_conn_id"] = "emr_default"

        # default for region_name
        if kwargs.get("region_name") is None:
            kwargs["region_name"] = Variable.get("AWS_REGION")

        # kwargs cleanup
        keys_to_remove = [key for key in kwargs if key.startswith("leucipa_")]
        for key in keys_to_remove:
            kwargs.pop(key)

        super().__init__(**kwargs)

    def terminator(self) -> EmrTerminateJobFlowOperator:
        if self._terminator is not None:
            return self._terminator

        AssertUtil.not_none(self.task_id, "Should not be None: `task_id`")
        AssertUtil.not_none(self.aws_conn_id, "Should not be None: `aws_conn_id`")
        AssertUtil.not_none(self.dag, "Should not be None: `dag`")

        job_flow_id = "{{ task_instance.xcom_pull(task_ids='" + self.task_id + "', key='return_value') }}"
        terminator_kwargs = {
            "task_id": f"{self.task_id}_terminator",
            "job_flow_id": job_flow_id,
            "aws_conn_id": self.aws_conn_id,
            "dag": self.dag,
            "trigger_rule": TriggerRule.ALL_DONE,
        }

        self._terminator = EmrTerminateJobFlowOperator(**terminator_kwargs)
        return self._terminator
