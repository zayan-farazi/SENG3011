import requests
import os
from jsonschema import validate
from tests.test_constants import HUB_ID_1, DATE_2
from constants import STATUS_OK, RISK_LOCATION_PATH
from .schemas.analytic_schema import ANALYTICS_API_SCHEMA

BASE_URL = os.environ["STAGING_BASE_URL"]

def test_risk_valid():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )
    res = response.json()
    assert response.status_code == STATUS_OK
    validate(instance=res,schema=ANALYTICS_API_SCHEMA)

