import os
import json
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