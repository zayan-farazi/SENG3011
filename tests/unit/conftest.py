import json
import boto3
import pytest
from moto import mock_aws
from test_constants import *
from constants import *
import os

# Sets up the mock S3 environment for unit testing, each test runs in isolation
# with a clean S3 state to simulate interactions with S3 without affecting real
# data.Pass setup_s3 as an argument to any test that needs to interact with S3
@pytest.fixture
def setup_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        os.environ["DATA_BUCKET"] = TEST_BUCKET_NAME

        # upload hubs.json to test bucket
        with open(HUBS_FILE_KEY, "r") as f:
            hubs = json.load(f)
        s3.put_object(
            Bucket=TEST_BUCKET_NAME,
            Key=HUBS_FILE_KEY,
            Body=json.dumps(hubs)
        )

        yield s3 