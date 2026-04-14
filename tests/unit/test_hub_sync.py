import json
import os
from unittest.mock import Mock, patch

from lambdas.hub_sync.handler import lambda_handler
from tests.test_constants import TEST_BUCKET_NAME


def _portwatch_response(features):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"features": features}
    return response


@patch.dict(
    os.environ,
    {
        "DATA_BUCKET": TEST_BUCKET_NAME,
        "PORTWATCH_HUBS_URL": "https://example.com/portwatch",
        "HUBS_RUNTIME_KEY": "runtime/hubs.json",
        "HUBS_SEED_KEY": "hubs.json",
        "HUBS_HISTORY_PREFIX": "history/hubs",
    },
    clear=False,
)
@patch("lambdas.hub_sync.handler.requests.get")
def test_hub_sync_success(mock_get, setup_s3):
    mock_get.side_effect = [
        _portwatch_response(
            [
                {
                    "attributes": {
                        "portid": "port-singapore",
                        "fullname": "Port of Singapore",
                        "country": "Singapore",
                        "lat": 1.264,
                        "lon": 103.82,
                        "LOCODE": "SGSIN",
                    }
                },
                {
                    "attributes": {
                        "portid": "port999",
                        "fullname": "Example Port",
                        "country": "Exampleland",
                        "lat": 10.123456,
                        "lon": 20.654321,
                        "LOCODE": "EXAMP",
                    }
                },
            ]
        ),
        _portwatch_response([]),
    ]

    response = lambda_handler({}, None)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["hub_count"] == 2
    assert body["runtime_key"] == "runtime/hubs.json"

    runtime_obj = setup_s3.get_object(Bucket=TEST_BUCKET_NAME, Key="runtime/hubs.json")
    runtime_hubs = json.loads(runtime_obj["Body"].read().decode("utf-8"))
    assert "H001" in runtime_hubs
    assert "PW_PORT999" in runtime_hubs

    history_listing = setup_s3.list_objects_v2(Bucket=TEST_BUCKET_NAME, Prefix="history/hubs/")
    assert history_listing["KeyCount"] == 1


@patch.dict(
    os.environ,
    {
        "DATA_BUCKET": TEST_BUCKET_NAME,
        "PORTWATCH_HUBS_URL": "https://example.com/portwatch",
        "HUBS_RUNTIME_KEY": "runtime/hubs.json",
        "HUBS_SEED_KEY": "hubs.json",
        "HUBS_HISTORY_PREFIX": "history/hubs",
    },
    clear=False,
)
@patch("lambdas.hub_sync.handler.requests.get")
def test_hub_sync_rejects_invalid_payload_and_keeps_last_good_runtime(mock_get, setup_s3):
    setup_s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="runtime/hubs.json",
        Body=json.dumps({"H001": {"name": "Port of Singapore", "lat": 1.264, "lon": 103.82}}),
        ContentType="application/json",
    )

    mock_get.side_effect = [_portwatch_response([{"attributes": {"portid": "bad-port", "fullname": "Broken Port"}}])]

    response = lambda_handler({}, None)

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "missing required hub fields" in body["error"]

    runtime_obj = setup_s3.get_object(Bucket=TEST_BUCKET_NAME, Key="runtime/hubs.json")
    runtime_hubs = json.loads(runtime_obj["Body"].read().decode("utf-8"))
    assert runtime_hubs == {"H001": {"name": "Port of Singapore", "lat": 1.264, "lon": 103.82}}
