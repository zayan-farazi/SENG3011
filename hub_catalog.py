import json
import os

import botocore

import constants


_HUB_CATALOG_CACHE = {}


def _get_hub_keys():
    runtime_key = os.environ.get("HUBS_RUNTIME_KEY", constants.HUBS_RUNTIME_KEY)
    seed_key = os.environ.get("HUBS_SEED_KEY", constants.HUBS_SEED_KEY)
    return runtime_key, seed_key


def load_hubs(s3, bucket_name):
    runtime_key, seed_key = _get_hub_keys()

    for key in (runtime_key, seed_key):
        try:
            metadata = s3.head_object(Bucket=bucket_name, Key=key)
            cache_key = (bucket_name, key)
            cache_entry = _HUB_CATALOG_CACHE.get(cache_key)
            cache_token = (
                metadata.get("ETag"),
                metadata.get("LastModified"),
                metadata.get("ContentLength"),
            )

            if cache_entry and cache_entry["token"] == cache_token:
                return cache_entry["hubs"]

            response = s3.get_object(Bucket=bucket_name, Key=key)
            hubs = json.loads(response["Body"].read().decode("utf-8"))
            _HUB_CATALOG_CACHE[cache_key] = {"token": cache_token, "hubs": hubs}
            return hubs
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
