import os
import json
STRIPE_ACCOUNTS = json.loads(
    os.getenv("STRIPE_ACCOUNTS_JSON", "{}")
)
ENDPOINT_RULES = {
    "customers": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "invoices": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "charges": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "payment_intents": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "plans": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "prices": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "products": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "subscriptions": {
            "type": "list",
            "pagination": True,
            "limit": True
    },
    "refunds": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "balance_transactions": {
        "type": "list",
        "pagination": True,
        "limit": True
    },
    "balance": {
        "type": "singleton",
        "pagination": False,
        "limit": False
    }
}
STRIPE_RESOURCES = [
    "customers", "subscriptions", "invoices", "charges", "payment_intents",
        "plans", "prices", "products", "refunds", "balance_transactions", "balance"
]