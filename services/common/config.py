import os
import json
# ---------------------------------------------------------------
PROJECT_ID = "apero-data-warehouse"
DATASET = "doba_dev"
TABLE_CONFIG = {
    "customers": {
        "mode": "json",
        "time_field": "created_at",
        "schema": [
            {"name": "id", "type": "STRING"},
            {"name": "open_id", "type": "STRING"},
            {"name": "created_at", "type": "DATETIME"},
            {"name": "call_at", "type": "DATETIME"},
            {"name": "data_json", "type": "JSON"},
        ]
    },
    "subscriptions": {
        "mode": "json",
        "time_field": "created_at",
        "schema": [
            {"name": "id", "type": "STRING"},
            {"name": "open_id", "type": "STRING"},
            {"name": "created_at", "type": "DATETIME"},
            {"name": "call_at", "type": "DATETIME"},
            {"name": "data_json", "type": "JSON"},
        ]
    },
    "invoices": {
            "mode": "json",
            "time_field": "created_at",
            "schema": [
                {"name": "id", "type": "STRING"},
                {"name": "open_id", "type": "STRING"},
                {"name": "created_at", "type": "DATETIME"},
                {"name": "call_at", "type": "DATETIME"},
                {"name": "data_json", "type": "JSON"},
            ]
    },
    "charges": {
                "mode": "json",
                "time_field": "created_at",
                "schema": [
                    {"name": "id", "type": "STRING"},
                    {"name": "open_id", "type": "STRING"},
                    {"name": "created_at", "type": "DATETIME"},
                    {"name": "call_at", "type": "DATETIME"},
                    {"name": "data_json", "type": "JSON"},
                ]
    },
    "payment_intents": {
                    "mode": "json",
                    "time_field": "created_at",
                    "schema": [
                        {"name": "id", "type": "STRING"},
                        {"name": "open_id", "type": "STRING"},
                    {"name": "created_at", "type": "DATETIME"},
                        {"name": "call_at", "type": "DATETIME"},
                        {"name": "data_json", "type": "JSON"},
                    ]
    },
    "plans": {
                "mode": "json",
                "time_field": "created_at",
                "schema": [
                    {"name": "id", "type": "STRING"},
                    {"name": "open_id", "type": "STRING"},
                    {"name": "created_at", "type": "DATETIME"},
                    {"name": "call_at", "type": "DATETIME"},
                    {"name": "data_json", "type": "JSON"},
                ]
    },
    "products": {
                "mode": "json",
                "time_field": "created_at",
                "schema": [
                    {"name": "id", "type": "STRING"},
                    {"name": "open_id", "type": "STRING"},
                    {"name": "created_at", "type": "DATETIME"},
                    {"name": "call_at", "type": "DATETIME"},
                    {"name": "data_json", "type": "JSON"},
                ]
    },
    "refunds": {
                    "mode": "json",
                    "time_field": "created_at",
                    "schema": [
                        {"name": "id", "type": "STRING"},
                        {"name": "open_id", "type": "STRING"},
                    {"name": "created_at", "type": "DATETIME"},
                        {"name": "call_at", "type": "DATETIME"},
                        {"name": "data_json", "type": "JSON"},
                    ]
    },
    "prices": {
        "mode": "json",
        "time_field": "created_at",
        "schema": [
            {"name": "id", "type": "STRING"},
            {"name": "open_id", "type": "STRING"},
            {"name": "created_at", "type": "DATETIME"},
            {"name": "call_at", "type": "DATETIME"},
            {"name": "data_json", "type": "JSON"},
        ]
    },
    "balance_transactions": {
                        "mode": "json",
                        "time_field": "created_at",
                        "schema": [
                            {"name": "id", "type": "STRING"},
                            {"name": "open_id", "type": "STRING"},
                            {"name": "created_at", "type": "DATETIME"},
                            {"name": "call_at", "type": "DATETIME"},
                            {"name": "data_json", "type": "JSON"},
                        ]
    },
    "subscription_items": {
                            "mode": "json",
                            "time_field": "created_at",
                            "schema": [
                                {"name": "id", "type": "STRING"},
                                {"name": "open_id", "type": "STRING"},
                                {"name": "created_at", "type": "DATETIME"},
                                {"name": "call_at", "type": "DATETIME"},
                                {"name": "data_json", "type": "JSON"},
                            ]
    },
    "balance": {
        "mode": "structured",
        "time_field": "call_at",
        "schema": [
            {"name": "open_id", "type": "STRING"},
            {"name": "call_at", "type": "DATETIME"},
            {"name": "available", "type": "JSON", "mode": "REPEATED"},
            {"name": "pending", "type": "JSON", "mode": "REPEATED"},
            {"name": "connect_reserved", "type": "JSON", "mode": "REPEATED"},
            {"name": "refund_and_dispute_prefunding", "type": "JSON"},
        ]
    }
}


# ----------------------------------------------------------------
BASE_URL = "https://api.stripe.com/v1"
STRIPE_ACCOUNTS = json.loads(
    os.getenv("STRIPE_ACCOUNTS_JSON", "{}")
)
LIST_CREATED_RULE = {
    "type": "list",
    "pagination": True,
    "limit": True,
    "modes": {
        "init": {
            "strategy": "full_load"
        },
        "daily": {
            "strategy": "incremental",
            "params": {
                "created[gte]": "{start}",
                "created[lte]": "{end}"
            }
        }
    }
}
LIST_SNAPSHOT_RULE = {
    "type": "singleton",
    "pagination": False,
    "limit": False,
    "modes": {
        "init": {
            "strategy": "snapshot"
        },
        "daily": {
            "strategy": "snapshot"
        }
    }
}

ENDPOINT_RULES = {
    "customers": LIST_CREATED_RULE,
    "invoices": LIST_CREATED_RULE,
    "charges": LIST_CREATED_RULE,
    "payment_intents": LIST_CREATED_RULE,
    "plans": LIST_CREATED_RULE,
    "prices": LIST_CREATED_RULE,
    "products": LIST_CREATED_RULE,
    "subscriptions": LIST_CREATED_RULE,
    #"subscription_items": LIST_CREATED_RULE,
    "refunds": LIST_CREATED_RULE,
    "balance_transactions": LIST_CREATED_RULE,
    "balance": LIST_SNAPSHOT_RULE
}
STRIPE_RESOURCES = list(ENDPOINT_RULES.keys())