from airflow.models import Variable

EMR_S3_BUCKET = Variable.get("EMR_S3_BUCKET")


class AwsEnvUtil:
    AA_INFRA = "AA_INFRA"
    AA_DEVQA = "AA_DEVQA"
    PS_INFRA = "PS_INFRA"
    PS_DEVQA = "PS_DEVQA"
    PS_PILOT = "PS_PILOT"

    @staticmethod
    def is_current_env_suffix(value: str) -> bool:
        return AwsEnvUtil.get_current_env_suffix() == value

    @staticmethod
    def get_current_env_suffix() -> str:
        """
        :return: Current AWS environment's suffix.
        """
        # TODO: improve the way AWS env suffix is detected
        if EMR_S3_BUCKET.startswith("971635488587"):
            return AwsEnvUtil.AA_INFRA  # Research InfraDev
        elif EMR_S3_BUCKET.startswith("322589048303"):
            return AwsEnvUtil.AA_DEVQA  # Research Dev/QA
        elif EMR_S3_BUCKET.startswith("481205689468"):
            return AwsEnvUtil.PS_INFRA  # Product InfraDev
        elif EMR_S3_BUCKET.startswith("504902519997"):
            return AwsEnvUtil.PS_DEVQA  # Product Dev/QA
        elif EMR_S3_BUCKET.startswith("232735049367"):
            return AwsEnvUtil.PS_PILOT  # Product Pilot
