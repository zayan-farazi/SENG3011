from tests.e2e.test_e2e_full_pipeline import test_e2e_full_pipeline, test_e2e_wrong_date
from constants import STATUS_OK

def lambda_handler(event, context):
    results = {}
    try:
        test_e2e_full_pipeline()
        results[test_e2e_full_pipeline.__name__] = "PASS"
    except AssertionError as e:
        results[test_e2e_full_pipeline.__name__] = f"FAIL: {str(e)}"

    try:
        test_e2e_wrong_date()
        results[test_e2e_wrong_date.__name__] = "PASS"
    except AssertionError as e:
        results[test_e2e_wrong_date.__name__] = f"FAIL: {str(e)}"

    return {
        "statusCode": STATUS_OK,
        "body": results
    }