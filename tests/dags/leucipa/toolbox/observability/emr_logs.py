import gzip
import logging
import time
from urllib.parse import urlparse

import boto3


def pull_logs(context):
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(format=log_format, level=logging.INFO)

    def find_s3_path(value):
        words = value.split()
        s3_url = ""
        for word in words:
            if word.startswith("s3://"):
                s3_url = word
        return s3_url

    def decode_bucket_name(bucket_name):
        return bucket_name.replace("***", "airflow")

    def find_bucket_and_key(s3_url):
        parsed_url = urlparse(s3_url)
        if parsed_url.scheme == "s3" and parsed_url.netloc:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.lstrip('/')
            logging.info("Bucket Name:", bucket_name)
            logging.info("Relative Path:", key)
            return bucket_name, key

        logging.error("Invalid S3 URL.")
        return ()

    s3_log_path = find_s3_path(str(context.get("exception")))
    bucket_name, key = find_bucket_and_key(s3_log_path)
    bucket_name = decode_bucket_name(bucket_name)
    s3 = boto3.client("s3")
    stdout_log_key = f"{key}stdout.gz"
    stderr_log_key = f"{key}stderr.gz"

    for i in range(0, 10):
        try:
            stdout_response = s3.get_object(Bucket=bucket_name, Key=stdout_log_key)
            with gzip.GzipFile(fileobj=stdout_response["Body"]) as gzipfile:
                stdout_content = gzipfile.read()
                logging.error(f"Log EMR stdout: {stdout_content}")

            stderr_response = s3.get_object(Bucket=bucket_name, Key=stderr_log_key)
            with gzip.GzipFile(fileobj=stderr_response["Body"]) as gzipfile:
                stderr_content = gzipfile.read()
                logging.error(f"Log EMR stderr: {stderr_content}")
            break
        except Exception as e:
            logging.error(f"Attempt {i}, Exception: {e}")
            time.sleep(30)