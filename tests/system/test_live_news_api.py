import os
import requests
import boto3

NEWS_API_BASE_URL = os.environ.get(
    "NEWS_API_BASE_URL",
    "https://i9pdxmupj7.execute-api.ap-southeast-2.amazonaws.com",
)

def test_live_news_api_flow():
    """
    End-to-end system test verifying the external News Sentiment API is online,
    issue API keys properly, and responds with valid sentiment data.
    """

    auth_url = f"{NEWS_API_BASE_URL}/api/auth/key"
    auth_resp = requests.post(auth_url, timeout=10)

    api_key = None
    if auth_resp.status_code in (200, 201):
        data = auth_resp.json()
        for field in ("key", "api_key", "apiKey", "token", "access_token"):
            if field in data:
                api_key = data[field]
                break
        if not api_key and isinstance(data, str) and len(data) > 8:
            api_key = data
    elif auth_resp.status_code == 409:
        # User already has an active key, pull from SSM as the handler would
        region = os.environ.get("AWS_REGION", "us-east-1")
        ssm = boto3.client("ssm", region_name=region)
        try:
            param = ssm.get_parameter(Name="/seng3011/news-api-key", WithDecryption=True)
            api_key = param["Parameter"]["Value"]
        except Exception as e:
            assert False, f"Auth returned 409 but failed to fetch key from SSM: {e}"
    else:
        assert False, f"Auth failed with {auth_resp.status_code}: {auth_resp.text}"



    assert api_key is not None, f"Could not extract API key from response: {data}"
    assert isinstance(api_key, str) and len(api_key) > 5, "Extracted API Key seems invalid"

    # Step 2: Fetch Sentiment using the new key
    sentiment_url = f"{NEWS_API_BASE_URL}/api/sentiment"
    params = {
        "keyword": "Singapore",
        "timeframe": "7d"
    }
    headers = {
        "x-api-key": api_key
    }

    sentiment_resp = requests.get(sentiment_url, params=params, headers=headers, timeout=15)

    assert sentiment_resp.status_code == 200, f"Sentiment fetch failed with {sentiment_resp.status_code}: {sentiment_resp.text}"

    # Step 3: Validate the response format matches the expected schema
    sentiment_data = sentiment_resp.json()

    expected_fields = ["country", "timeframe", "articleCount", "averageSentiment", "distribution"]

    # The API might return "keyword" instead of "country" as per the endpoint mock schema
    if "keyword" in sentiment_data and "country" not in sentiment_data:
        expected_fields[0] = "keyword"

    for field in expected_fields:
        assert field in sentiment_data, f"Missing required field '{field}' in response: {sentiment_data}"

    assert isinstance(sentiment_data["articleCount"], int), "articleCount should be an integer"
    assert isinstance(sentiment_data["averageSentiment"], (int, float)), "averageSentiment should be a number"
    assert "positive" in sentiment_data["distribution"], "distribution should contain 'positive'"
    assert "negative" in sentiment_data["distribution"], "distribution should contain 'negative'"
    assert "neutral" in sentiment_data["distribution"], "distribution should contain 'neutral'"

