import json
from datetime import datetime, timezone
from unittest.mock import patch
from lambdas.ingestion.handler import lambda_handler as ingestion_handler
from lambdas.retrieval.handler import lambda_handler as retrieval_handler
from constants import DATE_FORMAT, STATUS_OK, STATUS_NOT_FOUND
from tests.test_constants import HUB_ID_1, RAW_WEATHER_DATA_H1

@patch("lambdas.ingestion.handler.fetch_weather")
def test_ingestion_then_retrieval(mock_get, setup_s3):
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        data = json.load(f)
    
    # Modify data to check exact values later on
    data["latitude"] = 10
    data["longitude"] = 5
    mock_get.return_value = json.dumps(data)

    s3 = setup_s3["s3"] 
    bucket = setup_s3["bucket"]
    event = {"pathParameters": {"hub_id": HUB_ID_1}}

    resp = ingestion_handler(event, None)
    body = json.loads(resp["body"])
    assert resp["statusCode"] == STATUS_OK
    assert body["message"] == "Success"

    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(obj["Body"].read())
    assert "longitude" in raw_data
    assert "latitude" in raw_data
    assert raw_data["latitude"] == 10
    assert raw_data["longitude"] == 5
    assert "hourly" in raw_data

    retrieval_event = {
        "rawPath": f"/raw/{HUB_ID_1}",
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }

    ret_resp = retrieval_handler(retrieval_event, None)
    ret_body = json.loads(ret_resp["body"])
    assert ret_resp["statusCode"] == STATUS_OK
    assert ret_body["latitude"] == raw_data["latitude"]
    assert ret_body["longitude"] == raw_data["longitude"]
    assert ret_body["hourly"]["data"] == raw_data["hourly"]["data"]
    assert ret_body == raw_data

@patch("lambdas.ingestion.handler.fetch_weather")
def test_multiple_ingestion_overwrite(mock_fetch_weather, setup_s3):
    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)
        
    # first ingestion
    mock_fetch_weather.return_value = json.dumps(data)
    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    # second ingestion with modified data
    data["latitude"] = 999
    mock_fetch_weather.return_value = json.dumps(data)
    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)

    retrieval_event = {
        "rawPath": f"/raw/{HUB_ID_1}",
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }

    resp = retrieval_handler(retrieval_event, None)
    body = json.loads(resp["body"])
    # latest data should be returned
    assert body["latitude"] == 999

@patch("lambdas.ingestion.handler.fetch_weather")
def test_ingestion_then_retrieval_not_found(mock_fetch_weather, setup_s3):
    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)

    data["currently"]["time"] = int(datetime.now(timezone.utc).timestamp())
    mock_fetch_weather.return_value = json.dumps(data)

    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)
    wrong_date = "01-01-2000"

    retrieval_event = {
        "rawPath": f"/raw/{HUB_ID_1}",
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": wrong_date}
    }

    resp = retrieval_handler(retrieval_event, None)
    assert resp["statusCode"] == STATUS_NOT_FOUND