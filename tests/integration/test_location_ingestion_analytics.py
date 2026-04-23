import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from constants import (
    DATE_FORMAT,
    LOCATION_PATH,
    RETRIEVE_PROCESSED_WEATHER_PATH,
    STATUS_OK,
)
from lambdas.analytics.handler import lambda_handler as analytics_handler
from lambdas.ingestion.handler import lambda_handler as ingestion_handler
from lambdas.location.handler import lambda_handler as location_handler
from lambdas.processing.handler import lambda_handler as processing_handler
from lambdas.retrieval.handler import lambda_handler as retrieval_handler
from tests.test_constants import RAW_WEATHER_DATA_H1


def _create_location(lat, lon, name):
    response = location_handler(
        {
            "httpMethod": "POST",
            "body": json.dumps({"lat": lat, "lon": lon, "name": name}),
        },
        None,
    )
    assert response["statusCode"] == STATUS_OK
    return json.loads(response["body"])["hub_id"]


def _mock_http_response(lambda_response):
    mock_response = Mock()
    mock_response.status_code = lambda_response["statusCode"]
    mock_response.text = lambda_response["body"]
    mock_response.json.return_value = json.loads(lambda_response["body"])
    return mock_response


def _sentiment_response(country, timeframe, article_count, avg_sentiment):
    positive = int(article_count * max(0, (1 + avg_sentiment) / 2))
    return {
        "country": country,
        "timeframe": timeframe,
        "articleCount": article_count,
        "averageSentiment": avg_sentiment,
        "distribution": {
            "positive": positive,
            "neutral": 0,
            "negative": article_count - positive,
        },
    }


def _analytics_requests_side_effect(country):
    def side_effect(url, **kwargs):
        if "openstreetmap.org/reverse" in url:
            mock_response = Mock()
            mock_response.status_code = STATUS_OK
            mock_response.json.return_value = {"address": {"country": country}}
            return mock_response

        if "api/sentiment" in url:
            headers = kwargs.get("headers", {})
            params = kwargs.get("params", {})
            assert "x-api-key" in headers
            assert params.get("timeframe") == "7d"

            payload = _sentiment_response(country, "7d", 160, -0.2)
            mock_response = Mock()
            mock_response.status_code = STATUS_OK
            mock_response.text = json.dumps(payload)
            mock_response.json.return_value = payload
            return mock_response

        if LOCATION_PATH in url:
            hub_id = url.rstrip("/").split("/")[-1]
            return _mock_http_response(
                location_handler(
                    {
                        "httpMethod": "GET",
                        "pathParameters": {"hub_id": hub_id},
                    },
                    None,
                )
            )

        if RETRIEVE_PROCESSED_WEATHER_PATH in url:
            hub_id = url.rstrip("/").split("/")[-1]
            date = kwargs.get("params", {}).get("date")
            return _mock_http_response(
                retrieval_handler(
                    {
                        "rawPath": f"/{RETRIEVE_PROCESSED_WEATHER_PATH}/{hub_id}",
                        "pathParameters": {"hub_id": hub_id},
                        "queryStringParameters": {"date": date},
                    },
                    None,
                )
            )

        raise AssertionError(f"Unexpected requests.get call: {url}")

    return side_effect


@patch("lambdas.ingestion.handler.fetch_weather")
def test_location_create_then_ingestion_then_retrieval(mock_fetch_weather, setup_s3_dynamodb):
    s3 = setup_s3_dynamodb["s3"]
    bucket = setup_s3_dynamodb["bucket"]

    lat = -33.868
    lon = 151.209
    hub_id = _create_location(lat, lon, "Sydney Test Port")

    with open(RAW_WEATHER_DATA_H1, "r") as file:
        weather_data = json.load(file)

    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    weather_data["latitude"] = lat
    weather_data["longitude"] = lon
    mock_fetch_weather.return_value = json.dumps(weather_data)

    ingestion_response = ingestion_handler({"pathParameters": {"hub_id": hub_id}}, None)
    assert ingestion_response["statusCode"] == STATUS_OK

    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{hub_id}/{date_str}.json"
    stored_raw = json.loads(s3.get_object(Bucket=bucket, Key=raw_key)["Body"].read())

    retrieval_response = retrieval_handler(
        {
            "rawPath": f"/raw/{hub_id}",
            "pathParameters": {"hub_id": hub_id},
            "queryStringParameters": {"date": date_str},
        },
        None,
    )

    assert retrieval_response["statusCode"] == STATUS_OK
    assert json.loads(retrieval_response["body"]) == stored_raw
    assert stored_raw["latitude"] == lat
    assert stored_raw["longitude"] == lon


@patch("lambdas.analytics.handler._get_news_api_key", return_value="test-integration-key")
@patch("lambdas.ingestion.handler.fetch_weather")
def test_location_create_then_full_analytics_pipeline(
    mock_fetch_weather,
    mock_get_news_api_key,
    setup_s3_dynamodb,
):
    s3 = setup_s3_dynamodb["s3"]
    bucket = setup_s3_dynamodb["bucket"]

    lat = -33.868
    lon = 151.209
    hub_name = "Sydney Test Port"
    hub_id = _create_location(lat, lon, hub_name)

    with open(RAW_WEATHER_DATA_H1, "r") as file:
        weather_data = json.load(file)

    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    weather_data["currently"]["time"] = now_ts
    weather_data["latitude"] = lat
    weather_data["longitude"] = lon
    mock_fetch_weather.return_value = json.dumps(weather_data)

    ingestion_response = ingestion_handler({"pathParameters": {"hub_id": hub_id}}, None)
    assert ingestion_response["statusCode"] == STATUS_OK

    date_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(DATE_FORMAT)
    raw_key = f"raw/weather/{hub_id}/{date_str}.json"
    raw_data = json.loads(s3.get_object(Bucket=bucket, Key=raw_key)["Body"].read())

    processing_response = processing_handler({"body": json.dumps(raw_data)}, None)
    assert processing_response["statusCode"] == STATUS_OK

    with patch(
        "lambdas.analytics.handler.requests.get",
        side_effect=_analytics_requests_side_effect("Australia"),
    ):
        analytics_response = analytics_handler(
            {
                "pathParameters": {"hub_id": hub_id},
                "queryStringParameters": {"date": date_str},
            },
            None,
        )

    assert analytics_response["statusCode"] == STATUS_OK

    risk_key = f"risk/weather/{hub_id}/latest.json"
    stored_risk = json.loads(s3.get_object(Bucket=bucket, Key=risk_key)["Body"].read())
    response_body = json.loads(analytics_response["body"])

    assert response_body == stored_risk

    daily_events = [
        event for event in stored_risk["events"] if event["event_type"] == "daily_risk_assessment"
    ]
    outlook_event = next(
        event for event in stored_risk["events"] if event["event_type"] == "seven_day_outlook"
    )
    geo_event = next(
        event
        for event in stored_risk["events"]
        if event["event_type"] == "geopolitical_risk_assessment"
    )

    assert daily_events
    assert outlook_event["attribute"]["hub_id"] == hub_id
    assert outlook_event["attribute"]["hub_name"] == hub_name
    assert outlook_event["attribute"]["lat"] == lat
    assert outlook_event["attribute"]["lon"] == lon
    assert outlook_event["attribute"]["geo_weight"] == 0.35
    assert outlook_event["attribute"]["weather_weight"] == 0.65
    assert (
        outlook_event["attribute"]["combined_risk_score"]
        != outlook_event["attribute"]["outlook_risk_score"]
    )

    assert geo_event["attribute"]["hub_id"] == hub_id
    assert geo_event["attribute"]["country"] == "Australia"
    assert geo_event["attribute"]["data_available"] is True
    assert geo_event["attribute"]["country_scores"][0]["timeframes"]["7d"]["article_count"] == 160
