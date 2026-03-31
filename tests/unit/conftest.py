import json
import boto3
import pytest
from moto import mock_aws
from tests.test_constants import TEST_BUCKET_NAME
from constants import HUBS_FILE_KEY
import os


@pytest.fixture(autouse=True)
def set_data_bucket_env():
    original = os.environ.get("DATA_BUCKET")
    os.environ["DATA_BUCKET"] = TEST_BUCKET_NAME
    yield
    if original is None:
        os.environ.pop("DATA_BUCKET", None)
    else:
        os.environ["DATA_BUCKET"] = original

# Sets up the mock S3 environment for unit testing, each test runs in isolation
# with a clean S3 state to simulate interactions with S3 without affecting real
# data.Pass setup_s3 as an argument to any test that needs to interact with S3
@pytest.fixture
def setup_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        os.environ["API_BASE_URL"] = "http://test-api"

        # upload hubs.json to test bucket
        with open(HUBS_FILE_KEY, "r") as f:
            hubs = json.load(f)
        s3.put_object(
            Bucket=TEST_BUCKET_NAME,
            Key=HUBS_FILE_KEY,
            Body=json.dumps(hubs)
        )

        yield s3 
