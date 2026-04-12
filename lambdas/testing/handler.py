import json

from constants import STATUS_OK
from tests.e2e.test_e2e_full_pipeline import test_e2e_full_pipeline, test_e2e_dynamic_hub_pipeline, test_e2e_wrong_date


def lambda_handler(event, context):
    results = {}
    overall_status = "pass"

    try:
        test_e2e_full_pipeline()
        results[test_e2e_full_pipeline.__name__] = "PASS"
    except AssertionError as e:
        results[test_e2e_full_pipeline.__name__] = f"FAIL: {str(e)}"
        overall_status = "fail"
    except Exception as e:
        results[test_e2e_full_pipeline.__name__] = f"ERROR: {type(e).__name__}: {str(e)}"
        overall_status = "fail"

    try:
        test_e2e_dynamic_hub_pipeline()
        results[test_e2e_dynamic_hub_pipeline.__name__] = "PASS"
    except AssertionError as e:
        results[test_e2e_dynamic_hub_pipeline.__name__] = f"FAIL: {str(e)}"
        overall_status = "fail"
    except Exception as e:
        results[test_e2e_dynamic_hub_pipeline.__name__] = f"ERROR: {type(e).__name__}: {str(e)}"
        overall_status = "fail"

    try:
        test_e2e_wrong_date()
        results[test_e2e_wrong_date.__name__] = "PASS"
    except AssertionError as e:
        results[test_e2e_wrong_date.__name__] = f"FAIL: {str(e)}"
        overall_status = "fail"
    except Exception as e:
        results[test_e2e_wrong_date.__name__] = f"ERROR: {type(e).__name__}: {str(e)}"
        overall_status = "fail"

    return {
        "statusCode": STATUS_OK,
        "body": json.dumps({
            "status": overall_status,
            "results": results,
        }),
    }
