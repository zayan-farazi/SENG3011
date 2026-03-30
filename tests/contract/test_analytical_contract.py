import requests
import os
from jsonschema import validate
from test_constants import HUB_ID_1, HUB_INVALID, DATE_2
from constants import STATUS_OK, STATUS_BAD_REQUEST, RISK_LOCATION_PATH
from .schemas.analytic_schema import ANALYTICS_API_SCHEMA

BASE_URL = os.environ["DEV_BASE_URL"]

def test_risk_valid():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{HUB_ID_1}"
    response = requests.get(
        url,
        params={"date": DATE_2}
    )
    res = response.json()
    assert response.status_code == STATUS_OK
    validate(instance=res,schema=ANALYTICS_API_SCHEMA)
    
