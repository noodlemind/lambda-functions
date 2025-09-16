import json
import gzip
from typing import Iterator, Tuple, Dict
import boto3
from urllib.parse import urlparse

def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    u = urlparse(s3_uri)
    if u.scheme != "s3":
        raise ValueError("s3_uri must start with s3://")
    return u.netloc, u.path.lstrip("/")

def iter_ndjson(s3_uri: str, start_offset: int = 0) -> Iterator[Tuple[int, Dict]]:
    """Stream NDJSON from S3. Supports gzip if ContentEncoding=gzip or key endswith .gz."""
    s3 = boto3.client("s3")
    bucket, key = parse_s3_uri(s3_uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]

    # Decide gzip
    is_gz = obj.get("ContentEncoding", "") == "gzip" or key.endswith(".gz")

    if is_gz:
        # Download then stream (starter approach)
        data = body.read()
        for idx, line in enumerate(gzip.decompress(data).splitlines()):
            if idx < start_offset:
                continue
            if not line:
                continue
            yield (idx, json.loads(line))
    else:
        # Streaming lines
        for idx, raw in enumerate(body.iter_lines()):
            if idx < start_offset:
                continue
            if not raw:
                continue
            yield (idx, json.loads(raw))

def iter_json_array_small(s3_uri: str):
    """For small files only; loads whole array."""
    s3 = boto3.client("s3")
    bucket, key = parse_s3_uri(s3_uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return json.loads(data)
