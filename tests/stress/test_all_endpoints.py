
import pytest
from tests.test_constants import MAX_TIME_OUT, MED_TIME_OUT, LOW_TIME_OUT, HIGH_TIME_OUT
from tests.stress.stress_helper import (run_stress, create_location, list_locations, 
                                        get_location, remove_watchlist, retrieve_raw, 
                                        retrieve_processed,ingest_weather, process_weather, 
                                        risk_location, add_watchlist, get_watchlist_hubs, 
                                        get_watchlist_messages, optimal_path)
@pytest.mark.stress
def test_stress_create_location():
    run_stress(create_location, MED_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_list_locations():
    run_stress(list_locations, HIGH_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_get_location():
    run_stress(get_location, MED_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_retrieve_raw():
    run_stress(retrieve_raw, LOW_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_retrieve_processed():
    run_stress(retrieve_processed, LOW_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_ingest_weather():
    run_stress(ingest_weather, MED_TIME_OUT, MAX_TIME_OUT)


@pytest.mark.stress
def test_stress_process_weather():
    run_stress(process_weather, HIGH_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_risk_location():
    run_stress(risk_location, MED_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_add_watchlist():
    run_stress(add_watchlist, LOW_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_remove_watchlist():
    run_stress(remove_watchlist, LOW_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_get_watchlist_hubs():
    run_stress(get_watchlist_hubs, MED_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_get_watchlist_messages():
    run_stress(get_watchlist_messages, MED_TIME_OUT, MAX_TIME_OUT)

@pytest.mark.stress
def test_stress_optimal_path():
    run_stress(optimal_path, HIGH_TIME_OUT, MAX_TIME_OUT)
