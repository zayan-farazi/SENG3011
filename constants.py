DATE_FORMAT = "%d-%m-%Y"
PROCESSED_TIME_SLOTS = ["0000", "0600", "1200", "1800"]
HUBS_FILE_KEY = "hubs.json"

STATUS_OK = 200
STATUS_BAD_REQUEST = 400
STATUS_NOT_FOUND = 404
STATUS_INTERNAL_SERVER_ERROR = 500
STATUS_BAD_GATEWAY = 502

RETRIEVE_RAW_WEATHER_PATH = "ese/v1/retrieve/raw/weather"
RETRIEVE_PROCESSED_WEATHER_PATH = "ese/v1/retrieve/processed/weather"
RISK_LOCATION_PATH = "ese/v1/risk/location"
MODEL_S3_KEY = "models/risk_model.joblib"
INGEST_WEATHER_PATH = "/ese/v1/ingest/weather"
PROCESS_WEATHER_PATH = "/ese/v1/process/weather"
RISK_LOCATION_PATH = "/ese/v1/risk/location"
MODEL_S3_KEY = "models/risk_model.joblib"

## custom metrics constants
API_SERVICE = "ApiService"
WEATHER_SERVICE = "WeatherService"
RISK_SERVICE = "RiskService"

INGESTION_REQUESTS = "IngestionRequests"
WEATHER_API_ERRORS = "WeatherAPIErrors"
WEATHER_RECORDS_INGESTED = "WeatherRecordsIngested"
RISK_CALCULATIONS = "RiskCalculations"
WEATHER_RECORDS_PROCESSED = "WeatherRecordsProcessed"
RETRIEVAL_ERRORS = "RetrievalErrors"
DATA_REQUESTS = "DataRequests"
