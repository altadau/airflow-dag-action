from airflow.models import Variable

EMR_S3_BUCKET = Variable.get("EMR_S3_BUCKET")
ENV_TYPE_PREFIX = Variable.get("ENV_TYPE_PREFIX")

class AssetUtil:

    @staticmethod
    def adapt_to_s3(path: str) -> str:
        if path.startswith("s3://"):
            return path
        elif path.startswith("assets://"):
            relpath = path.split("assets://")[1]
            return f"s3://{EMR_S3_BUCKET}/{ENV_TYPE_PREFIX}-leucipa-data-ingest/aux/assets/{relpath}"
