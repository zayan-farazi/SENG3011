import pytest
import boto3
import json
import os
from test_constants import TEST_BUCKET_NAME, TEST_API_KEY, TEST_BASE_URL
from constants import HUBS_FILE_KEY, MODEL_S3_KEY
from moto import mock_aws

import io
import joblib  # type: ignore[import-untyped]
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import tempfile
import lambdas.analytics.handler as handler  # type: ignore[import-untyped]

def _create_dummy_model():
    np.random.seed(42)
    X = np.random.rand(20, 6).astype(np.float32)
    y = np.random.rand(20).astype(np.float32)
    model = RandomForestRegressor(n_estimators=2, random_state=42)
    model.fit(X, y)
    buf = io.BytesIO()
    joblib.dump(model, buf)
    buf.seek(0)
    return buf.read()

@pytest.fixture(autouse=True)
def reset_model_cache():
    handler._MODEL = None
    tmp = os.path.join(tempfile.gettempdir(), "risk_model.joblib")
    if os.path.exists(tmp):
        os.remove(tmp)
    yield
    handler._MODEL = None
    if os.path.exists(tmp):
        os.remove(tmp)


@pytest.fixture
def setup_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = TEST_BUCKET_NAME
        s3.create_bucket(Bucket=bucket)

        with open(HUBS_FILE_KEY, "r") as f:
            hubs = json.load(f)

        s3.put_object(
            Bucket=bucket,
            Key=HUBS_FILE_KEY,
            Body=json.dumps(hubs)
        )

        model_bytes = _create_dummy_model()
        s3.put_object(Bucket=bucket, Key=MODEL_S3_KEY, Body=model_bytes)

        os.environ["DATA_BUCKET"] = bucket
        os.environ["API_KEY"] = TEST_API_KEY
        os.environ["API_BASE_URL"] = TEST_BASE_URL

        yield {"s3": s3, "bucket": bucket}