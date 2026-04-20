import pytest
import boto3
import json
import os
from tests.test_constants import TEST_BUCKET_NAME, TEST_API_KEY, TEST_BASE_URL
from constants import HUBS_FILE_KEY, MODEL_S3_KEY
from moto import mock_aws

import io
import joblib  # type: ignore[import-untyped]
from sklearn.ensemble import RandomForestRegressor  # type: ignore[import-untyped]
import numpy as np
import tempfile
import lambdas.analytics.handler as handler
import constants

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
        dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)

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
        os.environ["AWS_REGION"] = constants.DEFAULT_REGION
        _create_location_table(dynamodb)
        _create_watchlist_table(dynamodb)

        yield {"s3": s3, "bucket": bucket}


def _create_location_table(dynamodb):
    table = dynamodb.create_table(
        TableName="locations",
        KeySchema=[{"AttributeName": "hub_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "hub_id", "AttributeType": "S"},
            {"AttributeName": "lat_lon", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "lat-lon-index",
                "KeySchema": [{"AttributeName": "lat_lon", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    table.wait_until_exists()


def _create_watchlist_table(dynamodb):
    table = dynamodb.create_table(
        TableName="watchlist",
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "hub_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "hub_id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "hub-id-index",
                "KeySchema": [
                    {"AttributeName": "hub_id", "KeyType": "HASH"},
                    {"AttributeName": "user_id", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    table.wait_until_exists()
