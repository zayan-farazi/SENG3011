import os
import json
from datetime import datetime
from unittest.mock import patch
from lambdas.ingestion.handler import lambda_handler as ingestion_handler
from lambdas.processing.handler import lambda_handler as processing_handler
from lambdas.retrieval.handler import lambda_handler as retrieval_handler

from tests.test_constants import HUB_ID_1, RAW_WEATHER_DATA_H1, PROCESSED_WEATHER_DATA_H1, DATE_H1
from constants import DATE_FORMAT, RETRIEVE_PROCESSED_WEATHER_PATH, STATUS_OK, STATUS_BAD_REQUEST, STATUS_INTERNAL_SERVER_ERROR
from datetime import timezone

@patch("lambdas.ingestion.handler.fetch_weather")
def test_ingestion_to_processing_success(mock_fetch_weather, setup_s3):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)
    mock_fetch_weather.return_value = json.dumps(data)

    # use ingestion to store data in s3
    resp = ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)
    assert resp["statusCode"] == STATUS_OK

    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"

    # take data from s3 and provides it to the processing
    raw_obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(raw_obj["Body"].read())
    resp = processing_handler({"body": json.dumps(raw_data)}, None)
    assert resp["statusCode"] == STATUS_OK
    processed_key = f"processed/weather/{HUB_ID_1}/{DATE_H1}.json"
    processed_obj = s3.get_object(Bucket=bucket, Key=processed_key)

    assert processed_obj is not None

    # Use retrieval lambda to check for process data retrieval in this case. 
    event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": DATE_H1}
    }

    response = retrieval_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    with open(PROCESSED_WEATHER_DATA_H1, "r") as f:
        processed_data = json.load(f)
    
    assert json.loads(response["body"]) == processed_data
    assert processed_data ==  json.loads(processed_obj['Body'].read().decode('utf-8'))


@patch("lambdas.ingestion.handler.fetch_weather")
def test_processing_multiple_overwrite(mock_fetch_weather, setup_s3):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    # ingestion
    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)

    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    data["currently"]["time"] = now_ts
    mock_fetch_weather.return_value = json.dumps(data)

    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"

    raw_obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(raw_obj["Body"].read())

    # first processing
    processing_handler({"body": json.dumps(raw_data)}, None)

    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    first = json.loads(
        s3.get_object(Bucket=bucket, Key=processed_key)["Body"].read()
    )

    # Change a data field
    raw_data["hourly"]["data"][0]["temperature"] += 10

    # second processing
    resp = processing_handler({"body": json.dumps(raw_data)}, None)
    assert resp["statusCode"] == STATUS_OK

    second = json.loads(
        s3.get_object(Bucket=bucket, Key=processed_key)["Body"].read()
    )

    assert first != second
    assert (
        first["days"][0]["snapshots"][0]["features"]["temperature"]
        != second["days"][0]["snapshots"][0]["features"]["temperature"]
    )

@patch("lambdas.ingestion.handler.fetch_weather")
def test_process_bad_data_ingested(mock_fetch_weather, setup_s3):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    bad_data = {
        "latitude": 10,
        "longitude": 20,
        # "hourly" field is lost
        "currently": {"time": int(datetime.now().timestamp())}
    }

    mock_fetch_weather.return_value = json.dumps(bad_data)

    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"

    raw_obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(raw_obj["Body"].read())

    resp = processing_handler({"body": json.dumps(raw_data)}, None)

    # Bad data ingested by ingestion can cause error to the processing, if the data is forwarded to the processing lambda.
    assert resp["statusCode"] == STATUS_BAD_REQUEST



@patch("lambdas.ingestion.handler.fetch_weather")
def test_processing_missing_env_config(mock_fetch_weather, setup_s3):

    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)

    data["currently"]["time"] = int(datetime.now(tz=timezone.utc).timestamp())
    mock_fetch_weather.return_value = json.dumps(data)

    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    os.environ.pop("DATA_BUCKET", "new")
    resp = processing_handler({"body": json.dumps(data)}, None)
    assert resp["statusCode"] == STATUS_INTERNAL_SERVER_ERROR