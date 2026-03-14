import json
from lambdas.processing.handler import lambda_handler
from test_constants import TEST_BUCKET_NAME, HUB_ID_1, RAW_WEATHER_DATA_H1, PROCESSED_WEATHER_DATA_H1, DATE_H1
from constants import STATUS_OK, STATUS_BAD_REQUEST, STATUS_NOT_FOUND, RETRIEVE_RAW_WEATHER_PATH

def test_post_process_valid(setup_s3):
    s3 = setup_s3
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        pirate_raw = json.load(f)
    
    with open(PROCESSED_WEATHER_DATA_H1, "r") as f:
        expected = json.load(f)
    event = {
        "body": json.dumps(pirate_raw),
    }
    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_OK
    processed_obj = s3.get_object(Bucket=TEST_BUCKET_NAME, Key= f"processed/weather/{HUB_ID_1}/{DATE_H1}.json")
    processed_data = json.loads(processed_obj['Body'].read().decode('utf-8'))

    assert processed_data== expected

def test_missing_body_and_records():
    event = {}
    response = lambda_handler(event, None)
    assert response["statusCode"] == STATUS_BAD_REQUEST

def test_hub_not_found(setup_s3):
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        pirate_raw = json.load(f)

    pirate_raw["latitude"] = 999
    pirate_raw["longitude"] = 999
    event = {
        "body": json.dumps(pirate_raw)
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_BAD_REQUEST

def test_hubs_file_missing(setup_s3):
    s3 = setup_s3
    s3.delete_object(
        Bucket=TEST_BUCKET_NAME,
        Key="hubs.json"
    )

    with open(RAW_WEATHER_DATA_H1) as f:
        pirate_raw = json.load(f)

    event = {
        "body": json.dumps(pirate_raw)
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == STATUS_NOT_FOUND

def test_invalid_json_body():
    event = {
        "body": "invalid-json"
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == 500
'''
# Uncomment and fix later on after deployment
def test_event_process_valid(setup_s3):
    s3 = setup_s3
    with open(RAW_WEATHER_DATA_FILE, "r") as f:
        pirate_raw = json.load(f)
    
    with open(PROCESSED_WEATHER_DATA_H1, "r") as f:
        expected = json.load(f)
    s3.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"raw/weather/{HUB_ID_1}/{DATE_1}.json",
        Body=json.dumps(pirate_raw)
    )

    event = {
        "Records": [
            {
                "eventSource": "aws:s3", 
                "s3": {
                    "object": {
                        "key":f"raw/weather/{HUB_ID_1}/{DATE_1}.json",
                    }

                }
            }
        ],
    }

    response = lambda_handler(event, None)

    processed_obj = s3.get_object(Bucket=TEST_BUCKET_NAME, Key= f"processed/weather/{HUB_ID_1}/{DATE_1}.json")
    processed_data = json.loads(processed_obj['Body'].read().decode('utf-8'))

    assert response["statusCode"] == STATUS_OK
    assert expected == processed_data
'''