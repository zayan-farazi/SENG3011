ANALYTICS_API_SCHEMA = {
    "type": "object",
    "required": ["data_source", "dataset_type", "dataset_id", "time_object", "events"],
    "properties": {
        "data_source": {"type": "string"},
        "dataset_type": {"type": "string"},
        "dataset_id": {"type": "string"},
        "time_object": {
            "type": "object",
            "required": ["timestamp", "timezone"],
            "properties": {
                "timestamp": {"type": "string"},
                "timezone": {"type": "string"}
            },
            "additionalProperties": False
        },
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["time_object", "event_type", "attribute"],
                "properties": {
                    "time_object": {
                        "type": "object",
                        "required": ["timestamp", "duration", "duration_unit", "timezone"],
                        "properties": {
                            "timestamp": {"type": "string"},
                            "duration": {"type": "number"},
                            "duration_unit": {"type": "string"},
                            "timezone": {"type": "string"}
                        },
                        "additionalProperties": False
                    },
                    "event_type": {"type": "string"},
                    "attribute": {
                        "type": "object",
                        "required": ["hub_id", "hub_name"],
                        "properties": {
                            "hub_id": {"type": "string"},
                            "hub_name": {"type": "string"},
                            "day": {"type": "number"},
                            "date": {"type": "string"},
                            "peak_risk_score": {"type": "number"},
                            "mean_risk_score": {"type": "number"},
                            "risk_level": {"type": "string"},
                            "primary_driver": {"type": "string"},
                            "worst_interval": {"type": "string"},
                            "snapshots": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "required": ["forecast_timestamp", "forecast_lead_hours", "risk_score", "risk_level", "primary_driver"],
                                    "properties": {
                                        "forecast_timestamp": {"type": "string"},
                                        "forecast_lead_hours": {"type": "number"},
                                        "risk_score": {"type": "number"},
                                        "risk_level": {"type": "string"},
                                        "primary_driver": {"type": "string"}
                                    },
                                    "additionalProperties": False
                                }
                            },
                            "lat": {"type": "number"},
                            "lon": {"type": "number"},
                            "outlook_risk_score": {"type": "number"},
                            "outlook_risk_level": {"type": "string"},
                            "peak_day": {"type": "string"},
                            "peak_day_number": {"type": "number"},
                            "forecast_origin": {"type": "string"},
                            "days_assessed": {"type": "number"}
                        },
                        "additionalProperties": True
                    }
                },
                "additionalProperties": False
            }
        }
    },
    "additionalProperties": False
}
