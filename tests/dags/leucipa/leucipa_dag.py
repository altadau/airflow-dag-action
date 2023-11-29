import inspect
from datetime import datetime, timedelta

from airflow import DAG


class LeucipaDAG(DAG):
    """
    Provides defaults to create Leucipa DAGs -
    with the same arguments as in DAG(..), and with provided arguments always taking priority.

    I.e. it could be used as a DAG's drop-in replacement::

        # Just use `LeucipaDAG` instead of `DAG`
        with LeucipaDAG(
            "my_dag",
            default_args={
                "owner": "airflow",
                "start_date": datetime(2020, 10, 17),
                ...
            },
            max_active_runs=2,
            schedule_interval="@daily",
            ...
        ) as dag:
            ...
    """

    LEUCIPA_DEFAULT_ARGS = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2020, 1, 1),
        "email": ["airflow@airflow.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    DEFAULT_TAG = "leucipa-data-ingest"

    VERSION_TAG = "AUTO_GIT_RELEASE_VERSION"

    def __init__(self, dag_id: str = None, **kwargs):

        # auto-define dag_id as caller's file's name
        if dag_id is None:
            current_frame = inspect.currentframe()
            caller_frame = inspect.getouterframes(current_frame, 2)
            caller_filename = caller_frame[1][1]
            caller_name = caller_filename.split("/")[-1].split(".")[0]
            dag_id = caller_name

        # define `leucipa_run_name` from `dag_id` and timestamp
        self.leucipa_run_timestamp = "{{ execution_date.timestamp() | int }}"
        self.leucipa_run_name = f"{dag_id}-{self.leucipa_run_timestamp}"

        # set `is_paused_upon_creation` as `True` by-default
        if kwargs.get("is_paused_upon_creation") is None:
            kwargs["is_paused_upon_creation"] = True

        # add "leucipa-data-ingest" tag
        if kwargs.get("tags") is None:
            kwargs["tags"] = []
        if LeucipaDAG.DEFAULT_TAG not in kwargs.get("tags"):
            kwargs.get("tags").append(LeucipaDAG.DEFAULT_TAG)
        if LeucipaDAG.VERSION_TAG not in kwargs.get("tags"):
            kwargs.get("tags").append(LeucipaDAG.VERSION_TAG)

        # set `catchup` as `False` by-default
        if kwargs.get("catchup") is None:
            kwargs["catchup"] = False

        # set `max_active_runs` as "1" by-default
        if kwargs.get("max_active_runs") is None:
            kwargs["max_active_runs"] = 1

        # merge `LEUCIPA_DEFAULT_ARGS` with client's `default_args` (client's take priority)
        if kwargs.get("default_args") is None:
            kwargs["default_args"] = {}
        kwargs["default_args"] = {**LeucipaDAG.LEUCIPA_DEFAULT_ARGS, **kwargs.get("default_args")}

        # kwargs cleanup
        keys_to_remove = [key for key in kwargs if key.startswith("leucipa_")]
        for key in keys_to_remove:
            kwargs.pop(key)

        # Ensures that `schedule_interval` is `None` and not `NOTSET` in the parent's constructor.
        if kwargs.get("schedule_interval") is None:
            schedule_interval = None
        else:
            schedule_interval = kwargs.get("schedule_interval")
            kwargs.pop("schedule_interval")

        super().__init__(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            **kwargs
        )
