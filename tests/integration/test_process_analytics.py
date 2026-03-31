import json
from datetime import datetime
from unittest.mock import patch, Mock
from lambdas.ingestion.handler import lambda_handler as ingestion_handler
from lambdas.processing.handler import lambda_handler as processing_handler
from lambdas.analytics.handler import lambda_handler as analytics_handler
from lambdas.retrieval.handler import lambda_handler as retrieval_handler
from tests.test_constants import HUB_ID_1, RAW_WEATHER_DATA_H1
from constants import DATE_FORMAT, STATUS_OK, RETRIEVE_PROCESSED_WEATHER_PATH
from datetime import timezone

def _mock_hub_info():
    return {"hub_id": HUB_ID_1, "name": "Test Hub", "lat": 1.264, "lon": 103.820}

def _mock_requests(mock_get, payload, status=STATUS_OK):
    mock_resp = Mock()
    mock_resp.status_code = status
    mock_resp.json.return_value = payload
    mock_resp.text = json.dumps(payload)
    mock_get.return_value = mock_resp

@patch("lambdas.analytics.handler.requests.get")
@patch("lambdas.retrieval.handler.validate_hub_id", return_value=True)
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_hub_info")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_ingestion_processing_analytics(
    mock_fetch_weather,
    mock_fetch_hub_info,
    mock_get_hub_info_from_pos,
    mock_validate_hub_id,
    mock_get_requests,
    setup_s3,
):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    mock_fetch_hub_info.return_value = _mock_hub_info()
    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Test Hub"}

    with open(RAW_WEATHER_DATA_H1, "r") as f:
        weather_data = json.load(f)
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    mock_fetch_weather.return_value = json.dumps(weather_data)

    ingestion_event = {"pathParameters": {"hub_id": HUB_ID_1}}
    ingestion_resp = ingestion_handler(ingestion_event, None)
    ingestion_body = json.loads(ingestion_resp["body"])
    assert ingestion_resp["statusCode"] == STATUS_OK
    assert ingestion_body["message"] == "Success"

    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)

    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    obj = s3.get_object(Bucket=bucket, Key=raw_key)
    raw_data = json.loads(obj["Body"].read())
    assert raw_data["currently"]["time"] == now_ts

    processing_event = {"body": json.dumps(raw_data)}
    processing_resp = processing_handler(processing_event, None)
    assert processing_resp["statusCode"] == STATUS_OK

    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    obj = s3.get_object(Bucket=bucket, Key=processed_key)
    processed_data = json.loads(obj["Body"].read())

    _mock_requests(mock_get_requests, processed_data)

    # Analytics handler should be able to interact with the processed path of date_str in mock s3
    analytics_event = {
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str},
    }
    analytics_resp = analytics_handler(analytics_event, None)
    assert analytics_resp["statusCode"] == STATUS_OK

    body = json.loads(analytics_resp["body"])
    assert "events" in body
    assert "data_source" in body
    assert "time_object" in body
    # basic check for risk assessment output
    daily_events = [e for e in body["events"] if e["event_type"] == "daily_risk_assessment"]
    assert len(daily_events) >= 1

@patch("lambdas.analytics.handler.requests.get")
@patch("lambdas.retrieval.handler.validate_hub_id", return_value=True)
@patch("lambdas.processing.handler.get_hub_info_from_pos")
@patch("lambdas.ingestion.handler.fetch_hub_info")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_processing_to_analytics_schema_break(
    mock_fetch_weather,
    mock_fetch_hub_info,
    mock_get_hub_info_from_pos,
    mock_validate_hub_id,
    mock_get,
    setup_s3,
):
    s3 = setup_s3["s3"]
    bucket = setup_s3["bucket"]

    mock_fetch_hub_info.return_value = _mock_hub_info()
    mock_get_hub_info_from_pos.return_value = {"hub_id": HUB_ID_1, "hub_name": "Test Hub"}

    # ingestion normal
    with open(RAW_WEATHER_DATA_H1) as f:
        data = json.load(f)

    data["currently"]["time"] = int(datetime.now(timezone.utc).timestamp())
    mock_fetch_weather.return_value = json.dumps(data)

    ingestion_handler({"pathParameters": {"hub_id": HUB_ID_1}}, None)

    # processing runs
    date_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{HUB_ID_1}/{date_str}.json"
    raw = json.loads(s3.get_object(Bucket=bucket, Key=raw_key)["Body"].read())
    processing_handler({"body": json.dumps(raw)}, None)

    # Succcess analytic
    retrieval_event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": date_str}
    }
    ret_resp = retrieval_handler(retrieval_event, None)
    mock_resp = Mock()
    mock_resp.status_code = STATUS_OK
    mock_resp.json.return_value =json.loads(ret_resp["body"])
    mock_resp.text = json.dumps(json.loads(ret_resp["body"]))
    mock_get.return_value = mock_resp
    _mock_requests(mock_get, json.loads(ret_resp["body"]))
    resp = analytics_handler({
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }, None)
    assert resp["statusCode"] == STATUS_OK

    # break processed data after processing (simulate corruption)
    processed_key = f"processed/weather/{HUB_ID_1}/{date_str}.json"
    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=json.dumps({"invalid": "schema"}) 
    )

    # Use retrieval to get the corrupted processed data and mock the corrupted data in the analytics. 
    # This parralels the real logic inside analytic lambda
    retrieval_event = {
        "rawPath": RETRIEVE_PROCESSED_WEATHER_PATH,
        "pathParameters": { "hub_id": HUB_ID_1 },
        "queryStringParameters": { "date": date_str}
    }
    ret_resp = retrieval_handler(retrieval_event, None)
    assert json.loads(ret_resp["body"]) == {"invalid": "schema"}
    _mock_requests(mock_get, json.loads(ret_resp["body"]))

    resp = analytics_handler({
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }, None)

    # cached check
    assert resp["statusCode"] == STATUS_OK
    s3.delete_object(
        Bucket=bucket,
        Key=f"risk/weather/{HUB_ID_1}/latest.json"
    )
    # Schema breaks
    resp = analytics_handler({
        "pathParameters": {"hub_id": HUB_ID_1},
        "queryStringParameters": {"date": date_str}
    }, None)
    assert resp["statusCode"] != STATUS_OK