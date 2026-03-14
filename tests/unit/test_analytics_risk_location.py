import json
import os
import io
import tempfile
import pytest
import numpy as np
import joblib
from unittest.mock import patch, Mock
from sklearn.ensemble import RandomForestRegressor
from moto import mock_aws
import boto3

from lambdas.analytics.handler import lambda_handler
import lambdas.analytics.handler as handler
from test_constants import (
    TEST_BUCKET_NAME, HUB_ID_1, HUB_INVALID, PROCESSED_WEATHER_DATA_FILE,
)
from constants import (
    STATUS_OK, STATUS_BAD_REQUEST, STATUS_INTERNAL_SERVER_ERROR,
    HUBS_FILE_KEY, MODEL_S3_KEY,
)


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
def setup_analytics_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        os.environ["DATA_BUCKET"] = TEST_BUCKET_NAME
        os.environ["API_BASE_URL"] = "http://test-api"

        with open(HUBS_FILE_KEY, "r") as f:
            hubs = json.load(f)
        s3.put_object(Bucket=TEST_BUCKET_NAME, Key=HUBS_FILE_KEY, Body=json.dumps(hubs))

        model_bytes = _create_dummy_model()
        s3.put_object(Bucket=TEST_BUCKET_NAME, Key=MODEL_S3_KEY, Body=model_bytes)

        yield s3


def _mock_retrieval_response():
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        data = json.load(f)
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = data
    mock_resp.text = json.dumps(data)
    return mock_resp


@patch("lambdas.analytics.handler.requests.get")
def test_valid_risk_request(mock_get, setup_analytics_s3):
    mock_get.return_value = _mock_retrieval_response()

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_OK

    body = json.loads(result["body"])
    assert "events" in body
    assert "data_source" in body
    assert "time_object" in body
    assert body["dataset_type"] == "Supply Chain Disruption Risk Assessment"

    daily_events = [e for e in body["events"] if e["event_type"] == "daily_risk_assessment"]
    outlook_events = [e for e in body["events"] if e["event_type"] == "seven_day_outlook"]
    assert len(daily_events) >= 1
    assert len(outlook_events) == 1

    day = daily_events[0]["attribute"]
    assert "peak_risk_score" in day
    assert "risk_level" in day
    assert "snapshots" in day
    assert day["risk_level"] in ("Low", "Elevated", "High", "Critical")

    for s in day["snapshots"]:
        assert "risk_score" in s
        assert "risk_level" in s
        assert "primary_driver" in s
        assert 0.0 <= s["risk_score"] <= 1.0


def test_missing_hub_id():
    event = {
        "pathParameters": {},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(result["body"]) == {"error": "Missing hub_id"}


def test_invalid_hub_id(setup_analytics_s3):
    event = {
        "pathParameters": {"hub_id": HUB_INVALID},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert json.loads(result["body"]) == {"error": "Invalid hub_id"}


@patch("lambdas.analytics.handler.requests.get")
def test_retrieval_fails(mock_get, setup_analytics_s3):
    mock_resp = Mock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal server error"
    mock_get.return_value = mock_resp

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == 502


@patch("lambdas.analytics.handler.requests.get")
def test_model_not_found(mock_get, setup_analytics_s3):
    s3 = setup_analytics_s3
    s3.delete_object(Bucket=TEST_BUCKET_NAME, Key=MODEL_S3_KEY)
    mock_get.return_value = _mock_retrieval_response()

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_INTERNAL_SERVER_ERROR


@patch("lambdas.analytics.handler.requests.get")
def test_missing_feature(mock_get, setup_analytics_s3):
    with open(PROCESSED_WEATHER_DATA_FILE, "r") as f:
        data = json.load(f)
    del data["days"][0]["snapshots"][0]["features"]["temperature"]

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = data
    mock_get.return_value = mock_resp

    event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": "10-03-2026"},
    }

    result = lambda_handler(event, None)
    assert result["statusCode"] == STATUS_BAD_REQUEST
    assert "temperature" in json.loads(result["body"])["error"]
