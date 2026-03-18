PROJECT_ID = "apero-data-warehouse"
DATASET = "doba_dev"
TABLE_CONFIG = {
    "customers": {
        "mode": "json",
        "schema": [
            {"name": "id", "type": "STRING"},
            {"name": "call_at", "type": "DATETIME"},
            {"name": "data_json", "type": "JSON"},
        ]
    },
    "subscriptions": {
        "mode": "json",
        "schema": [
            {"name": "id", "type": "STRING"},
            {"name": "call_at", "type": "DATETIME"},
            {"name": "data_json", "type": "JSON"},
        ]
    },
    "invoices": {
            "mode": "json",
            "schema": [
                {"name": "id", "type": "STRING"},
                {"name": "call_at", "type": "DATETIME"},
                {"name": "data_json", "type": "JSON"},
            ]
    },
    "charges": {
                "mode": "json",
                "schema": [
                    {"name": "id", "type": "STRING"},
                    {"name": "call_at", "type": "DATETIME"},
                    {"name": "data_json", "type": "JSON"},
                ]
    },
    "payment_intents": {
                    "mode": "json",
                    "schema": [
                        {"name": "id", "type": "STRING"},
                        {"name": "call_at", "type": "DATETIME"},
                        {"name": "data_json", "type": "JSON"},
                    ]
    },
    "plans": {
                "mode": "json",
                "schema": [
                    {"name": "id", "type": "STRING"},
                    {"name": "call_at", "type": "DATETIME"},
                    {"name": "data_json", "type": "JSON"},
                ]
    },
    "products": {
                "mode": "json",
                "schema": [
                    {"name": "id", "type": "STRING"},
                    {"name": "call_at", "type": "DATETIME"},
                    {"name": "data_json", "type": "JSON"},
                ]
    },
    "refunds": {
                    "mode": "json",
                    "schema": [
                        {"name": "id", "type": "STRING"},
                        {"name": "call_at", "type": "DATETIME"},
                        {"name": "data_json", "type": "JSON"},
                    ]
    },
    "balance_transactions": {
                        "mode": "json",
                        "schema": [
                            {"name": "id", "type": "STRING"},
                            {"name": "call_at", "type": "DATETIME"},
                            {"name": "data_json", "type": "JSON"},
                        ]
    },
    "subscription_items": {
                            "mode": "json",
                            "schema": [
                                {"name": "id", "type": "STRING"},
                                {"name": "call_at", "type": "DATETIME"},
                                {"name": "data_json", "type": "JSON"},
                            ]
    },
    "balance": {
        "mode": "structured",
        "schema": [
            {"name": "call_at", "type": "DATETIME"},
            {"name": "available", "type": "JSON", "mode": "REPEATED"},
            {"name": "pending", "type": "JSON", "mode": "REPEATED"},
            {"name": "refund_and_dispute_prefunding", "type": "JSON", "mode": "REPEATED"},
        ]
    }
}