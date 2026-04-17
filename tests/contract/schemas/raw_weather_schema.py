INGESTION_API_SCHEMA = {
    "type": "object",
    "required": ["message"],
    "properties": {
        "message": {
            "type": "string",
            "enum": ["Success"]
        }
    },
    "additionalProperties": False
}


RAW_WEATHER_SCHEMA = {
    "type": "object",
    "properties": {
        "latitude": {"type": "number"},
        "longitude": {"type": "number"},
        "timezone": {"type": "string"},
        "offset": {"type": "number"},
        "elevation": {"type": "number"},
        "currently": {
            "type": "object",
            "properties": {
                "time": {"type": "number"},
                "summary": {"type": "string"},
                "icon": {"type": "string"},
                "nearestStormDistance": {"type": "number"},
                "nearestStormBearing": {"type": "number"},
                "precipIntensity": {"type": "number"},
                "precipProbability": {"type": "number"},
                "precipIntensityError": {"type": "number"},
                "precipType": {"type": "string"},
                "rainIntensity": {"type": "number"},
                "snowIntensity": {"type": "number"},
                "iceIntensity": {"type": "number"},
                "temperature": {"type": "number"},
                "apparentTemperature": {"type": "number"},
                "dewPoint": {"type": "number"},
                "humidity": {"type": "number"},
                "pressure": {"type": "number"},
                "windSpeed": {"type": "number"},
                "windGust": {"type": "number"},
                "windBearing": {"type": "number"},
                "cloudCover": {"type": "number"},
                "uvIndex": {"type": "number"},
                "visibility": {"type": "number"},
                "ozone": {"type": "number"},
                "smoke": {"type": "number"},
                "fireIndex": {"type": "number"},
                "feelsLike": {"type": "number"},
                "currentDayIce": {"type": "number"},
                "currentDayLiquid": {"type": "number"},
                "currentDaySnow": {"type": "number"},
                "stationPressure": {"type": "number"},
                "solar": {"type": "number"},
                "cape": {"type": "number"}
            },
            "required": ["time", "temperature", "humidity", "pressure", "windSpeed"]
        },
        "minutely": {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "icon": {"type": "string"},
                "data": {"type": "array"}
            }
        },
        "hourly": {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "icon": {"type": "string"},
                "data": {"type": "array"}
            }
        },
        "day_night": {
            "type": "object",
            "properties": {"data": {"type": "array"}}
        },
        "daily": {"type": "object", "properties": {"data": {"type": "array"}}},
        "alerts": {"type": "array"},
        "flags": {"type": "object"}
    },
    "required": ["latitude", "longitude", "timezone", "currently"]
}
