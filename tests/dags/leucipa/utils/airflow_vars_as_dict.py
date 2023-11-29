import os
import sys
from collections.abc import Mapping

from airflow.models import Variable

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================


class AirflowVarsAsDict(Mapping):
    """
    Provide access to Airflow variables as dictionary (non-modifiable and non-iterable).
    """

    EXTENSIONS = {}

    @staticmethod
    def extend_with(items):
        """
        (!) For testing purposes only
        """
        AirflowVarsAsDict.EXTENSIONS = {**AirflowVarsAsDict.EXTENSIONS, **items}

    def __getitem__(self, key):
        value = Variable.get(key=key, default_var=None)
        if value is None:
            value = AirflowVarsAsDict.EXTENSIONS.get(key)
            if value is None:
                raise KeyError(f"{key}")
        return value

    def __setitem__(self, key, value):
        raise TypeError("Cannot update dictionary of Airflow variables")

    def __contains__(self, key):
        variable_is_not_none = Variable.get(key=key, default_var=None) is not None
        extension_is_not_none = AirflowVarsAsDict.EXTENSIONS.get(key) is not None
        return variable_is_not_none or extension_is_not_none

    def __iter__(self):
        raise TypeError("Cannot iterate through dictionary of Airflow variables")

    def __len__(self):
        raise TypeError("Cannot determine length of dictionary of Airflow variables")
