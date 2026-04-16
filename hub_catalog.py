import json
import os

import botocore

import constants


def _get_hub_keys():
    runtime_key = os.environ.get("HUBS_RUNTIME_KEY", constants.HUBS_RUNTIME_KEY)
    seed_key = os.environ.get("HUBS_SEED_KEY", constants.HUBS_SEED_KEY)
    return runtime_key, seed_key


def load_hubs(s3, bucket_name):
    runtime_key, seed_key = _get_hub_keys()

    for key in (runtime_key, seed_key):
        try:
            response = s3.get_object(Bucket=bucket_name, Key=key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404", "NotFound"}:
                continue
            raise

    raise FileNotFoundError(
        f"Hub catalog not found in bucket {bucket_name}. "
        f"Tried runtime key '{runtime_key}' and seed key '{seed_key}'."
    )


def load_seed_hubs(s3, bucket_name):
    _, seed_key = _get_hub_keys()
    response = s3.get_object(Bucket=bucket_name, Key=seed_key)
    return json.loads(response["Body"].read().decode("utf-8"))
