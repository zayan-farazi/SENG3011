import json
from lambdas.processing.handler import lambda_handler
from tests.test_constants import TEST_BUCKET_NAME, HUB_ID_1, RAW_WEATHER_DATA_H1, PROCESSED_WEATHER_DATA_H1, DATE_H1
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, STATUS_INTERNAL_SERVER_ERROR
from unittest.mock import patch, Mock


def _mock_retrieval_response():
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        data = json.load(f)
    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_resp.json.return_value = data
    mock_resp.text = json.dumps(data)
    return mock_resp


def test_post_process_valid(setup_s3):
    s3 = setup_s3
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        pirate_raw = json.load(f)
    
    with open(PROCESSED_WEATHER_DATA_H1, "r") as f:
        expected = json.load(f)
    event = {
        "body": json.dumps(pirate_raw),
    }
    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    processed_obj = s3.get_object(Bucket=TEST_BUCKET_NAME, Key= f"processed/weather/{HUB_ID_1}/{DATE_H1}.json")
    processed_data = json.loads(processed_obj['Body'].read().decode('utf-8'))

    assert processed_data== expected
    assert json.loads(json.loads(response["body"])["processed_data"])== expected

def test_missing_body_and_records():
    event = {}
    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST

def test_hub_not_found(setup_s3):
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        pirate_raw = json.load(f)

    pirate_raw["latitude"] = 999
    pirate_raw["longitude"] = 999
    event = {
        "body": json.dumps(pirate_raw)
    }
    response = lambda_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert "No hub found" in body["error"]
    

def test_invalid_json_body():
    event = {
        "body": "invalid-json"
    }

    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert "Expecting value" in body["error"]


def test_post_invalid_missing_top_key():
    # No "currently"
    event_body = {
        "latitude": 1.0,
        "longitude": 2.0,
        "hourly": {"data": []}
    }

    event = {"body": json.dumps(event_body)}

    response = lambda_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert "Missing key:" in body["error"]

def test_post_invalid_hourly_data_type():
    event_body = {
        "currently": {"time": 123456},
        "latitude": 1.0,
        "longitude": 2.0,
        "hourly": {"data": "not-a-list"}
    }

    event = {"body": json.dumps(event_body)}

    response = lambda_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert "must be a list" in body["error"]

def test_post_invalid_hourly_entry_missing_key():
    event_body = {
        "currently": {"time": 123456},
        "latitude": 1.0,
        "longitude": 2.0,
        "hourly": {
            "data": [
                {"time": 123456, "temperature": 25}  # missing windSpeed, windGust, etc.
            ]
        }
    }

    event = {"body": json.dumps(event_body)}

    response = lambda_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert "Missing key in hourly" in body["error"]

def test_post_invalid_hourly_time_type():
    event_body = {
        "currently": {"time": 123456},
        "latitude": 1.0,
        "longitude": 2.0,
        "hourly": {
            "data": [
                {"time": "not-a-number", "temperature": 25, "windSpeed": 5, "windGust": 7, "precipIntensity": 0, "pressure": 1010, "humidity": 0.5}
            ]
        }
    }

    event = {"body": json.dumps(event_body)}

    response = lambda_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == STATUS_BAD_REQUEST
    assert "must be a number" in body["error"]

@patch("lambdas.processing.handler.requests.get")
def test_event_process_valid(mock_get, setup_s3):
    s3 = setup_s3
    mock_get.return_value = _mock_retrieval_response()
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        pirate_raw = json.load(f)
    
    with open(PROCESSED_WEATHER_DATA_H1, "r") as f:
        expected = json.load(f)
    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"raw/weather/{HUB_ID_1}/{DATE_H1}.json",
        Body=json.dumps(pirate_raw)
    )
    event = {
        "Records": [
            {
                "eventSource": "aws:s3", 
                "s3": {
                    "object": {
                        "key":f"raw/weather/{HUB_ID_1}/{DATE_H1}.json",
                    }

                }
            }
        ],
    }

    response = lambda_handler(event, None)
    assert response == [{"status": "processed","processed_data": expected }]
    mock_get.assert_called_once_with(
        "http://test-api/ese/v1/retrieve/raw/weather/H001",
        params={"date": DATE_H1},
        timeout=10,
    )

    processed_obj = s3.get_object(Bucket=TEST_BUCKET_NAME, Key= f"processed/weather/{HUB_ID_1}/{DATE_H1}.json")
    processed_data = json.loads(processed_obj['Body'].read().decode('utf-8'))

    assert expected == processed_data
    

@patch("lambdas.processing.handler.requests.get")
def test_event_retrieval_404(mock_get, setup_s3):
    mock_resp = Mock()
    mock_resp.status_code = STATUS_NOT_FOUND
    mock_resp.text = "not found"
    mock_get.return_value = mock_resp

    event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "object": {
                        "key": f"raw/weather/{HUB_ID_1}/{DATE_H1}.json"
                    }
                }
            }
        ]
    }

    response = lambda_handler(event, None)

    assert response[0]["status"] == "error"
    assert "Raw weather data not found" in response[0]["error"]


@patch("lambdas.processing.handler.requests.get")
def test_event_retrieval_service_error(mock_get, setup_s3):
    mock_resp = Mock()
    mock_resp.status_code = STATUS_INTERNAL_SERVER_ERROR
    mock_resp.text = "server error"
    mock_get.return_value = mock_resp

    event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "object": {
                        "key": f"raw/weather/{HUB_ID_1}/{DATE_H1}.json"
                    }
                }
            }
        ]
    }

    response = lambda_handler(event, None)

    assert response[0]["status"] == "error"
    assert "Retrieval service returned 500" in response[0]["error"]


@patch("lambdas.processing.handler.requests.get")
def test_invalid_s3_key(mock_get):
    mock_get.return_value = _mock_retrieval_response()

    event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "object": {
                        "key": "invalid-key-format"
                    }
                }
            }
        ]
    }

    response = lambda_handler(event, None)

    assert response[0]["status"] == "error"
