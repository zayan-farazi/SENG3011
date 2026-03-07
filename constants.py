import os

BUCKET_NAME = os.environ.get("DATA_BUCKET", "seng-3011-bkt-zayan-dev")

DATE_FORMAT = "%d-%m-%Y"
PROCESSED_TIME_SLOTS = ["0000", "0600", "1200", "1800"]


## TEST CONSTANTS ##
TEST_BUCKET_NAME = "test-bucket"

HUB_ID_1 = "hub1"
HUB_INVALID = "invalid_hub"

DATE_1 = "10-03-2026"
DATE_INVALID = "invalid-date"

RAW_WEATHER_DATA_FILE = "tests/data/pirate_raw_sample.json"
PROCESSED_WEATHER_DATA_FILE = "tests/data/pirate_processed_sample.json"

STATUS_OK = 200
STATUS_BAD_REQUEST = 400
STATUS_NOT_FOUND = 404
STATUS_INTERNAL_SERVER_ERROR = 500