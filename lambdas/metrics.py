import json
import time

def log_metric(name, value, service, unit="Count"):
    print(json.dumps({
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "SupplyChainRiskSystem",
                    "Dimensions": [["Service"]],
                    "Metrics": [
                        {"Name": name, "Unit": unit}
                    ]
                }
            ]
        },
        "Service": service,
        name: value
    }))