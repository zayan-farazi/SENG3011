"""
Comprehensive tests for the News Sentiment API client layer.

Tests cover the full lifecycle of how the analytics handler interacts
with the external news sentiment API, including:
    - API key management (cache, SSM, registration, invalidation)
    - HTTP request construction (headers, params, timeouts)
    - Response parsing and sentiment-to-risk conversion
    - Error handling (401, 403, 404, 500, timeouts, malformed JSON)
    - Retry-with-refresh logic (_get_geopolitical_risk_with_retry)
    - Sequential fetching (to avoid 401 rate limiting)
    - Rate limiting / graceful degradation
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from lambdas.analytics import handler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sentiment_resp(keyword, timeframe, count, avg_sent):
    """Build a mock news API response."""
    pos = int(count * max(0, (1 + avg_sent) / 2))
    return {
        "keyword": keyword,
        "timeframe": timeframe,
        "articleCount": count,
        "averageSentiment": avg_sent,
        "distribution": {"positive": pos, "neutral": 0, "negative": count - pos},
    }


def _mock_resp(status, body):
    """Create a mock requests.Response."""
    m = MagicMock()
    m.status_code = status
    m.json.return_value = body
    m.text = json.dumps(body) if isinstance(body, dict) else str(body)
    return m


@pytest.fixture(autouse=True)
def reset_api_key_cache():
    """Reset the module-level API key cache between tests."""
    original = handler._NEWS_API_KEY
    yield
    handler._NEWS_API_KEY = original


# ===========================================================================
# 1. API KEY MANAGEMENT
# ===========================================================================

class TestAPIKeyManagement:
    """Tests for the three-tier key resolution: cache -> SSM -> register."""

    def test_returns_cached_key_when_available(self):
        handler._NEWS_API_KEY = "cached-key-123"
        result = handler._get_news_api_key()
        assert result == "cached-key-123"

    @patch("lambdas.analytics.handler._register_new_news_api_key", return_value=None)
    @patch("lambdas.analytics.handler._load_key_from_ssm", return_value="ssm-key-456")
    def test_loads_from_ssm_when_no_cache(self, mock_ssm, mock_reg):
        handler._NEWS_API_KEY = None
        result = handler._get_news_api_key()
        assert result == "ssm-key-456"
        assert handler._NEWS_API_KEY == "ssm-key-456"
        mock_ssm.assert_called_once()
        mock_reg.assert_not_called()

    @patch("lambdas.analytics.handler._persist_key_to_ssm")
    @patch("lambdas.analytics.handler._register_new_news_api_key", return_value="new-key-789")
    @patch("lambdas.analytics.handler._load_key_from_ssm", return_value=None)
    def test_registers_new_key_when_ssm_empty(self, mock_ssm, mock_reg, mock_persist):
        handler._NEWS_API_KEY = None
        result = handler._get_news_api_key()
        assert result == "new-key-789"
        assert handler._NEWS_API_KEY == "new-key-789"
        mock_persist.assert_called_once_with("new-key-789")

    @patch("lambdas.analytics.handler._register_new_news_api_key", return_value=None)
    @patch("lambdas.analytics.handler._load_key_from_ssm", return_value=None)
    def test_returns_none_when_all_sources_fail(self, mock_ssm, mock_reg):
        handler._NEWS_API_KEY = None
        result = handler._get_news_api_key()
        assert result is None

    def test_invalidate_clears_cache(self):
        handler._NEWS_API_KEY = "active-key"
        handler._invalidate_news_api_key()
        assert handler._NEWS_API_KEY is None


class TestKeyRegistration:
    """Tests for _register_new_news_api_key POST endpoint."""

    @patch("lambdas.analytics.handler.requests.post")
    def test_successful_registration(self, mock_post):
        mock_post.return_value = _mock_resp(201, {"key": "brand-new-key"})
        result = handler._register_new_news_api_key()
        assert result == "brand-new-key"

    @patch("lambdas.analytics.handler.requests.post")
    def test_handles_409_conflict(self, mock_post):
        mock_post.return_value = _mock_resp(409, {"message": "Key already exists"})
        result = handler._register_new_news_api_key()
        assert result is None

    @patch("lambdas.analytics.handler.requests.post")
    def test_handles_network_error(self, mock_post):
        mock_post.side_effect = Exception("Connection refused")
        result = handler._register_new_news_api_key()
        assert result is None

    @patch("lambdas.analytics.handler.requests.post")
    def test_handles_alternative_key_field_names(self, mock_post):
        """The handler checks for 'key', 'api_key', 'apiKey', 'token', 'access_token'."""
        for field in ("api_key", "apiKey", "token", "access_token"):
            mock_post.return_value = _mock_resp(200, {field: "found-it"})
            result = handler._register_new_news_api_key()
            assert result == "found-it", f"Failed for field '{field}'"


# ===========================================================================
# 2. HTTP REQUEST CONSTRUCTION
# ===========================================================================

class TestHTTPRequestConstruction:
    """Verify _fetch_sentiment builds the request correctly."""

    @patch("lambdas.analytics.handler.requests.get")
    def test_uses_x_api_key_header(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("test", "24h", 10, 0.0))
        handler._fetch_sentiment("test", "24h", "my-key")

        args, kwargs = mock_get.call_args
        assert kwargs["headers"]["x-api-key"] == "my-key"
        assert "key" not in kwargs["params"]

    @patch("lambdas.analytics.handler.requests.get")
    def test_passes_keyword_and_timeframe_as_params(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("China", "7d", 50, -0.2))
        handler._fetch_sentiment("China", "7d", "key")

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["keyword"] == "China"
        assert kwargs["params"]["timeframe"] == "7d"

    @patch("lambdas.analytics.handler.requests.get")
    def test_uses_correct_base_url(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("test", "24h", 0, 0))
        handler._fetch_sentiment("test", "24h", "key")

        url = mock_get.call_args[0][0]
        assert "/api/sentiment" in url
        assert handler.NEWS_API_BASE_URL in url

    @patch("lambdas.analytics.handler.requests.get")
    def test_sets_timeout(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("test", "24h", 0, 0))
        handler._fetch_sentiment("test", "24h", "key")

        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == handler.SENTIMENT_TIMEOUT_SECONDS


# ===========================================================================
# 3. RESPONSE PARSING & RISK CONVERSION
# ===========================================================================

class TestResponseParsing:
    """Test how API responses are parsed and converted to risk scores."""

    @patch("lambdas.analytics.handler.requests.get")
    def test_parses_article_count(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("China", "7d", 115, -0.3))
        result = handler._fetch_sentiment("China", "7d", "key")
        assert result["article_count"] == 115

    @patch("lambdas.analytics.handler.requests.get")
    def test_parses_average_sentiment(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("China", "7d", 50, -0.42))
        result = handler._fetch_sentiment("China", "7d", "key")
        assert result["avg_sentiment"] == -0.42

    @patch("lambdas.analytics.handler.requests.get")
    def test_parses_distribution(self, mock_get):
        resp = _sentiment_resp("China", "7d", 100, 0.0)
        resp["distribution"] = {"positive": 30, "neutral": 40, "negative": 30}
        mock_get.return_value = _mock_resp(200, resp)
        result = handler._fetch_sentiment("China", "7d", "key")
        assert result["distribution"]["positive"] == 30
        assert result["distribution"]["neutral"] == 40
        assert result["distribution"]["negative"] == 30

    @patch("lambdas.analytics.handler.requests.get")
    def test_risk_conversion_boundaries(self, mock_get):
        """Verify (1 - sentiment) / 2 mapping across the full range."""
        test_cases = [
            (-1.0, 1.0),   # Most negative -> highest risk
            (-0.5, 0.75),
            (0.0, 0.5),    # Neutral -> mid risk
            (0.5, 0.25),
            (1.0, 0.0),    # Most positive -> lowest risk
        ]
        for sentiment, expected_risk in test_cases:
            mock_get.return_value = _mock_resp(200, _sentiment_resp("X", "24h", 50, sentiment))
            result = handler._fetch_sentiment("X", "24h", "key")
            assert result["risk_score"] == expected_risk, (
                f"sentiment={sentiment}: expected risk={expected_risk}, got={result['risk_score']}"
            )

    @patch("lambdas.analytics.handler.requests.get")
    def test_zero_articles_still_returns_valid_result(self, mock_get):
        mock_get.return_value = _mock_resp(200, _sentiment_resp("X", "24h", 0, 0))
        result = handler._fetch_sentiment("X", "24h", "key")
        assert result is not None
        assert result["article_count"] == 0
        assert result["risk_score"] == 0.5


# ===========================================================================
# 4. ERROR HANDLING
# ===========================================================================

class TestErrorHandling:
    """Test all error paths in _fetch_sentiment."""

    @patch("lambdas.analytics.handler.requests.get")
    def test_401_does_not_invalidate_key(self, mock_get):
        """A 401 is a rate-limit signal, NOT a real auth failure. Key stays."""
        handler._NEWS_API_KEY = "stale-key"
        mock_get.return_value = _mock_resp(401, {"code": 401, "message": "Invalid key"})
        result = handler._fetch_sentiment("X", "7d", "stale-key")
        assert result is None
        assert handler._NEWS_API_KEY == "stale-key"

    @patch("lambdas.analytics.handler.requests.get")
    def test_403_does_not_invalidate_key(self, mock_get):
        """A 403 is treated the same as 401 — rate-limit, not auth failure."""
        handler._NEWS_API_KEY = "forbidden-key"
        mock_get.return_value = _mock_resp(403, {"code": 403, "message": "Forbidden"})
        result = handler._fetch_sentiment("X", "7d", "forbidden-key")
        assert result is None
        assert handler._NEWS_API_KEY == "forbidden-key"

    @patch("lambdas.analytics.handler.requests.get")
    def test_404_returns_none(self, mock_get):
        mock_get.return_value = _mock_resp(404, {"error": "Not found"})
        result = handler._fetch_sentiment("X", "24h", "key")
        assert result is None

    @patch("lambdas.analytics.handler.requests.get")
    def test_500_returns_none(self, mock_get):
        mock_get.return_value = _mock_resp(500, {"error": "Internal server error"})
        result = handler._fetch_sentiment("X", "24h", "key")
        assert result is None

    @patch("lambdas.analytics.handler.requests.get")
    def test_timeout_returns_none(self, mock_get):
        import requests  # type: ignore
        mock_get.side_effect = requests.exceptions.Timeout("Read timed out")
        result = handler._fetch_sentiment("X", "24h", "key")
        assert result is None

    @patch("lambdas.analytics.handler.requests.get")
    def test_connection_error_returns_none(self, mock_get):
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("DNS failed")
        result = handler._fetch_sentiment("X", "24h", "key")
        assert result is None

    @patch("lambdas.analytics.handler.requests.get")
    def test_malformed_json_returns_none(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        mock_resp.text = "not json"
        mock_get.return_value = mock_resp
        result = handler._fetch_sentiment("X", "24h", "key")
        assert result is None


# ===========================================================================
# 5. RETRY-WITH-REFRESH LOGIC
# ===========================================================================

class TestRetryWithRefresh:
    """Test _get_geopolitical_risk_with_retry key refresh behavior."""

    @patch("lambdas.analytics.handler._get_geopolitical_risk")
    @patch("lambdas.analytics.handler._get_news_api_key")
    def test_no_retry_when_data_available(self, mock_get_key, mock_geo):
        mock_get_key.return_value = "good-key"
        mock_geo.return_value = {
            "data_available": True,
            "geopolitical_risk_score": 0.6,
            "country": "China",
        }
        geo_meta = {"country": "China", "keywords": ["China"]}
        result = handler._get_geopolitical_risk_with_retry(geo_meta)
        assert result["data_available"] is True
        assert mock_geo.call_count == 1  # No retry

    @patch("lambdas.analytics.handler._get_geopolitical_risk")
    @patch("lambdas.analytics.handler._get_news_api_key")
    def test_retries_once_when_key_invalidated(self, mock_get_key, mock_geo):
        """When first attempt fails (key invalidated), retry with fresh key."""
        mock_get_key.side_effect = ["stale-key", "fresh-key"]

        # First call: no data, key was invalidated
        first_result = {
            "data_available": False,
            "geopolitical_risk_score": 0.5,
            "country": "China",
            "keyword_scores": [],
            "geopolitical_risk_level": "Elevated",
        }
        # Second call: success with fresh key
        second_result = {
            "data_available": True,
            "geopolitical_risk_score": 0.7,
            "country": "China",
        }
        mock_geo.side_effect = [first_result, second_result]

        # Simulate key invalidation
        handler._NEWS_API_KEY = None

        geo_meta = {"country": "China", "keywords": ["China"]}
        result = handler._get_geopolitical_risk_with_retry(geo_meta)
        assert result["data_available"] is True
        assert mock_geo.call_count == 2

    @patch("lambdas.analytics.handler._get_news_api_key", return_value=None)
    def test_returns_neutral_when_no_key_at_all(self, mock_get_key):
        geo_meta = {"country": "China", "keywords": ["China"]}
        result = handler._get_geopolitical_risk_with_retry(geo_meta)
        assert result["data_available"] is False
        assert result["geopolitical_risk_score"] == 0.5


# ===========================================================================
# 6. COUNTRY FETCHING
# ===========================================================================

class TestCountryFetching:
    """Test that _get_geopolitical_risk fires request correctly."""

    @patch("lambdas.analytics.handler._fetch_sentiment")
    def test_fires_country_fetch_sequentially(self, mock_fetch):
        mock_fetch.return_value = {
            "risk_score": 0.5, "article_count": 10,
            "avg_sentiment": 0.0, "distribution": {},
        }
        geo_meta = {
            "country": "Singapore"
        }
        handler._get_geopolitical_risk(geo_meta, "test-key")
        assert mock_fetch.call_count == 1  # 1 country x 1 timeframe (7d)

    @patch("lambdas.analytics.handler._fetch_sentiment", return_value=None)
    def test_all_failures_returns_neutral(self, mock_fetch):
        geo_meta = {"country": "Mars"}
        result = handler._get_geopolitical_risk(geo_meta, "test-key")
        assert result["data_available"] is False
        assert result["geopolitical_risk_score"] == 0.5


# ===========================================================================
# 7. GEO META RESOLUTION (preset vs dynamic)
# ===========================================================================

class TestGeoMetaResolution:
    """Test _resolve_geo_meta for preset and dynamic hubs."""

    def test_all_8_preset_hubs_resolve(self):
        for hub_id in ["H001", "H002", "H003", "H004", "H005", "H006", "H007", "H008"]:
            meta = handler._resolve_geo_meta(hub_id, 0.0, 0.0)
            assert meta["country"] != "Unknown", f"{hub_id} resolved to Unknown"

    def test_preset_hub_ignores_coordinates(self):
        """Preset hubs use hardcoded meta regardless of lat/lon."""
        meta = handler._resolve_geo_meta("H001", 999.0, 999.0)
        assert meta["country"] == "Singapore"

    @patch("lambdas.analytics.handler._reverse_geocode_country", return_value="Germany")
    def test_dynamic_hub_uses_reverse_geocoding(self, mock_geo):
        meta = handler._resolve_geo_meta("LOC_custom_123", 50.1, 8.7)
        assert meta["country"] == "Germany"
        mock_geo.assert_called_once_with(50.1, 8.7)

    @patch("lambdas.analytics.handler._reverse_geocode_country", return_value=None)
    def test_dynamic_hub_falls_back_to_unknown(self, mock_geo):
        """When Nominatim fails, fallback to Unknown."""
        meta = handler._resolve_geo_meta("LOC_ocean", 1.0, 105.0)
        assert meta["country"] == "Unknown"


# ===========================================================================
# 9. FULL GEO RISK OUTPUT STRUCTURE
# ===========================================================================

class TestGeoRiskOutputStructure:
    """Verify the complete output dict from _get_geopolitical_risk."""

    @patch("lambdas.analytics.handler._fetch_sentiment")
    def test_output_has_all_required_fields(self, mock_fetch):
        mock_fetch.return_value = {
            "risk_score": 0.6, "article_count": 50,
            "avg_sentiment": -0.2, "distribution": {},
        }
        geo_meta = {"country": "China"}
        result = handler._get_geopolitical_risk(geo_meta, "key")

        required_fields = [
            "country", "geopolitical_risk_score", "geopolitical_risk_level",
            "country_scores", "data_available",
        ]
        for field in required_fields:
            assert field in result, f"Missing field: {field}"

        # Country scores
        for c in result["country_scores"]:
            assert "country" in c
            assert "data_available" in c
            if c["data_available"]:
                assert "composite_risk_score" in c
                assert "timeframes" in c


class TestNeutralGeoRisk:
    """Test the fallback neutral risk object."""

    def test_structure(self):
        result = handler._neutral_geo_risk("TestCountry")
        assert result["country"] == "TestCountry"
        assert result["geopolitical_risk_score"] == 0.5
        assert result["geopolitical_risk_level"] == "Elevated"
        assert result["data_available"] is False
        assert result["country_scores"] == []
