import json
import boto3
from decimal import Decimal

from hub_catalog import load_hubs
from hub_lookup import resolve_hub
from tests.test_constants import TEST_BUCKET_NAME
import constants


def test_load_hubs_falls_back_to_seed(setup_s3):
    hubs = load_hubs(setup_s3, TEST_BUCKET_NAME)

    assert "H001" in hubs
    assert hubs["H001"]["name"] == "Port of Singapore"


def test_load_hubs_prefers_runtime_catalog(setup_s3):
    runtime_hubs = {
        "H001": {"name": "Port of Singapore", "lat": 1.264, "lon": 103.82},
        "PW_PORT999": {"name": "Example Port", "lat": 10.5, "lon": 20.5},
    }
    setup_s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key="runtime/hubs.json",
        Body=json.dumps(runtime_hubs),
        ContentType="application/json",
    )

    hubs = load_hubs(setup_s3, TEST_BUCKET_NAME)

    assert "PW_PORT999" in hubs
    assert hubs["PW_PORT999"]["name"] == "Example Port"


def test_resolve_hub_prefers_dynamic_hub(setup_s3_dynamodb):
    table = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION).Table("locations")
    table.put_item(
        Item={
            "hub_id": "LOC_TEST01",
            "lat_lon": "12.345:67.890",
            "name": "Dynamic Port",
            "lat": Decimal("12.345"),
            "lon": Decimal("67.890"),
            "type": "dynamic",
            "created_at": "2026-04-15T00:00:00Z",
        }
    )

    hub = resolve_hub("LOC_TEST01", TEST_BUCKET_NAME, s3=setup_s3_dynamodb)

    assert hub == {
        "hub_id": "LOC_TEST01",
        "name": "Dynamic Port",
        "lat": 12.345,
        "lon": 67.89,
        "type": "dynamic",
    }


def test_resolve_hub_falls_back_to_monitored(setup_s3):
    hub = resolve_hub("H001", TEST_BUCKET_NAME, s3=setup_s3)

    assert hub == {
        "hub_id": "H001",
        "name": "Port of Singapore",
        "lat": 1.264,
        "lon": 103.82,
        "type": "monitored",
    }
