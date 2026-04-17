PROCESSING_API_SCHEMA = {
    "type": "object",
    "required": ["message", "processed_data"],
    "properties": {
        "message": {"type": "string"},
        "processed_data": {"type": "object"}  # validated separately
    },
    "additionalProperties": True
}

PROCESSED_DATA_SCHEMA = {
    "type": "object",
    "required": [
        "schema_version",
        "hub_id",
        "hub_name",
        "lat",
        "lon",
        "forecast_origin",
        "days"
    ],
    "properties": {
        "schema_version": {"type": "string"},
        "hub_id": {"type": "string"},
        "hub_name": {"type": "string"},
        "lat": {"type": "number"},
        "lon": {"type": "number"},
        "forecast_origin": {"type": "string"},
        "days": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["date", "day", "snapshots"],
                "properties": {
                    "date": {"type": "string"},
                    "day": {"type": "number"},
                    "snapshots": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": [
                                "forecast_timestamp",
                                "forecast_lead_hours",
                                "features"
                            ],
                            "properties": {
                                "forecast_timestamp": {"type": "string"},
                                "forecast_lead_hours": {"type": "number"},
                                "features": {
                                    "type": "object",
                                    "required": [
                                        "temperature",
                                        "wind_speed",
                                        "wind_gust",
                                        "precip_intensity",
                                        "pressure",
                                        "humidity"
                                    ],
                                    "properties": {
                                        "temperature": {"type": "number"},
                                        "wind_speed": {"type": "number"},
                                        "wind_gust": {"type": "number"},
                                        "precip_intensity": {"type": "number"},
                                        "pressure": {"type": "number"},
                                        "humidity": {"type": "number"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
