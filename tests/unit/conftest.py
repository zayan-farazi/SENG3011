import boto3
import pytest
from moto import mock_s3
from constants import *

# Sets up the mock S3 environment for testing, each test runs in isolation with
# a clean S3 state to simulate interactions with S3 without affecting real data.
# Pass setup_s3 as an argument to any test that needs to interact with S3
@pytest.fixture(autouse=True)
def setup_s3():
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        os.environ["DATA_BUCKET"] = TEST_BUCKET_NAME
        yield s3 