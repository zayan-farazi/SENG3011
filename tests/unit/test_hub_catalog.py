import json

from hub_catalog import load_hubs
from test_constants import TEST_BUCKET_NAME


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
