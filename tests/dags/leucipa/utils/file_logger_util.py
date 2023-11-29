import sys
import os
import base64
import gzip
import json
from io import BytesIO
from typing import List

# ============== REQUIRED ==============
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# ======================================

from leucipa.utils.s3_util import S3Util


class LoggedFile:

    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.content = content


class FileLoggerUtil:

    @staticmethod
    def read_from_s3(s3_path: str) -> LoggedFile:
        filename = s3_path.split("/")[-1]
        content = S3Util.read_as_bytes(s3_path)
        return LoggedFile(filename=filename, content=content)

    @staticmethod
    def file_to_text(logged_file: LoggedFile) -> str:
        # Compress the content using gzip
        with BytesIO() as bytes_buffer:
            with gzip.GzipFile(fileobj=bytes_buffer, mode='wb') as gzip_file:
                gzip_file.write(logged_file.content)
            compressed_content = bytes_buffer.getvalue()
        # Encode the compressed content to base64
        base64_body = base64.b64encode(compressed_content).decode('utf-8')

        result = "\n"
        result += "#### BASE64_FILE ####\n"
        result += "{\n"
        result += '    "filename": "{' + logged_file.filename + '}",\n'
        result += '    "base64_body": "{' + base64_body + '}"\n'
        result += "}\n"
        result += "#### /BASE64_FILE ####\n"

        return result

    @staticmethod
    def text_to_file(format_body: str) -> LoggedFile:
        json_body = format_body \
            .replace("#### BASE64_FILE ####", "") \
            .replace("#### /BASE64_FILE ####", "") \
            .strip()

        dict_body = json.loads(json_body)
        filename = dict_body["filename"]
        base64_body = dict_body["base64_body"]

        # Decode the base64 content
        base64_bytes = base64.b64decode(base64_body)
        # Decompress the content using gzip
        with gzip.GzipFile(fileobj=BytesIO(base64_bytes), mode='rb') as gzip_file:
            content = gzip_file.read()
            return LoggedFile(filename=filename, content=content)

    @staticmethod
    def log_to_files(log_text: str) -> List[LoggedFile]:
        result = []
        text_parts = FileLoggerUtil._find_text_parts(log_text, "#### BASE64_FILE ####", "#### /BASE64_FILE ####")
        for text in text_parts:
            logged_file = FileLoggerUtil.text_to_file(text)
            result.append(logged_file)
        return result

    @staticmethod
    def _find_text_parts(text: str, beg: str, end: str) -> List[str]:
        parts = []
        start = 0
        while True:
            # Find the start of the pattern
            start_index = text.find(beg, start)
            if start_index == -1:
                break  # No more patterns found
            # Find the end of the pattern
            end_index = text.find(end, start_index + len(beg))
            if end_index == -1:
                break  # No end found for the current pattern
            # Extract the substring, including 'beg' and 'end'
            part = text[start_index:end_index + len(end)]
            parts.append(part)
            # Move start to the end of the current pattern to find further patterns
            start = end_index + len(end)
        return parts
