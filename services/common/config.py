"""
Tập trung cấu hình pipeline Stripe → BigQuery (một nơi duy nhất).

Không cần thêm `config.py` trong từng package (bigquery/stripe/…): mọi module
chỉ import từ đây để dễ tìm và sửa.

Các khối (theo thứ tự trong file):
  - PROJECT / DATASET
  - TABLE_CONFIG (schema bảng stripe_raw_*)
  - STRIPE_ACCOUNTS (env)
  - Rule gọi API Stripe + ENDPOINT_RULES + STRIPE_RESOURCES
"""

import json
import os

# --- Project / warehouse -----------------------------------------------------
PROJECT_ID = "apero-data-warehouse"
DATASET = "doba_dev"

# --- BigQuery raw landing tables --------------------------------------------
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
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
        ],
    },
}

# --- Stripe accounts (env) ---------------------------------------------------
STRIPE_ACCOUNTS = json.loads(os.getenv("STRIPE_ACCOUNTS_JSON", "{}"))

# --- Stripe API --------------------------------------------------------------
BASE_URL = "https://api.stripe.com/v1"

LIST_CREATED_RULE = {
    "type": "list",
    "pagination": True,
    "limit": True,
    "modes": {
        "init": {"strategy": "full_load"},
        "daily": {
            "strategy": "incremental",
            "params": {
                "created[gte]": "{start}",
                "created[lte]": "{end}",
            },
        },
    },
}

LIST_SNAPSHOT_RULE = {
    "type": "singleton",
    "pagination": False,
    "limit": False,
    "modes": {
        "init": {"strategy": "snapshot"},
        "daily": {"strategy": "snapshot"},
    },
}

LIST_BY_PARENT_RULE = {
    "type": "list",
    "pagination": True,
    "limit": True,
    "modes": {
        "init": {"strategy": "by_parent"},
        "daily": {"strategy": "by_parent"},
    },
}

ENDPOINT_RULES = {
    "customers": LIST_CREATED_RULE,
    "invoices": LIST_CREATED_RULE,
    "charges": LIST_CREATED_RULE,
    "payment_intents": LIST_CREATED_RULE,
    "plans": LIST_CREATED_RULE,
    "prices": LIST_CREATED_RULE,
    "products": LIST_CREATED_RULE,
    "subscriptions": {
        **LIST_CREATED_RULE,
        "children": ["subscription_items"],
    },
    "subscription_items": {
        **LIST_BY_PARENT_RULE,
        "include_in_dag": False,
        "parent_resource": "subscriptions",
        "parent_id_field": "id",
        "parent_param": "subscription",
    },
    "refunds": LIST_CREATED_RULE,
    "balance_transactions": LIST_CREATED_RULE,
    "balance": LIST_SNAPSHOT_RULE,
}

STRIPE_RESOURCES = [
    resource
    for resource, rule in ENDPOINT_RULES.items()
    if rule.get("include_in_dag", True)
]

__all__ = [
    "PROJECT_ID",
    "DATASET",
    "TABLE_CONFIG",
    "STRIPE_ACCOUNTS",
    "BASE_URL",
    "LIST_CREATED_RULE",
    "LIST_SNAPSHOT_RULE",
    "LIST_BY_PARENT_RULE",
    "ENDPOINT_RULES",
    "STRIPE_RESOURCES",
]
