from constants import STATUS_OK
import os
import importlib.util

# Use relative path to avoid module errors
e2e_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../tests/e2e/test_e2e_full_pipeline.py"))
spec = importlib.util.spec_from_file_location("test_e2e_full_pipeline", e2e_file)
e2e_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(e2e_module)

test_e2e_full_pipeline = e2e_module.test_e2e_full_pipeline
test_e2e_wrong_date = e2e_module.test_e2e_wrong_date

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