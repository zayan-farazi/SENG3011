import json
import os
import boto3
import pytest
from moto import mock_aws
from tests.test_constants import TEST_BUCKET_NAME
import constants


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
# data. Pass setup_s3 as an argument to any test that needs to interact with S3
@pytest.fixture
def setup_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name=constants.DEFAULT_REGION)
        s3.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": constants.DEFAULT_REGION},
        )
        os.environ["API_BASE_URL"] = "http://test-api"

        # upload hubs.json to test bucket
        with open(constants.HUBS_FILE_KEY, "r") as f:
            hubs = json.load(f)
        s3.put_object(
            Bucket=TEST_BUCKET_NAME,
            Key=constants.HUBS_FILE_KEY,
            Body=json.dumps(hubs)
        )

        yield s3 

@pytest.fixture
def setup_dynamodb():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
        _create_location_table(dynamodb)
        _create_watchlist_table(dynamodb)
        os.environ["API_BASE_URL"] = "http://test-api"
        yield

@pytest.fixture
def setup_s3_dynamodb():
    with mock_aws():
        s3 = boto3.client("s3", region_name=constants.DEFAULT_REGION)
        s3.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": constants.DEFAULT_REGION},
        )
        os.environ["API_BASE_URL"] = "http://test-api"

        dynamodb = boto3.resource("dynamodb", region_name=constants.DEFAULT_REGION)
        _create_location_table(dynamodb)

        yield s3 

def _create_location_table(dynamodb):
    table = dynamodb.create_table(
        TableName="locations",
        KeySchema=[{"AttributeName": "hub_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "hub_id", "AttributeType": "S"},
            {"AttributeName": "lat_lon", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "lat-lon-index",
                "KeySchema": [{"AttributeName": "lat_lon", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    table.wait_until_exists()

def _create_watchlist_table(dynamodb):
    dynamodb.create_table(
        TableName="watchlist",
        KeySchema=[
            {"AttributeName": "hub_id", "KeyType": "HASH"},
            {"AttributeName": "email", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "hub_id", "AttributeType": "S"},
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
