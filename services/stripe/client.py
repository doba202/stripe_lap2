import requests
import os
from .config import ENDPOINT_RULES
BASE_URL = "https://api.stripe.com/v1"

class StripeClient:
    def __init__(self, api_key=None):
        self.api_key = api_key

    def get(self, endpoint, params=None):
        print('api_key',self.api_key)
        response = requests.get(
            f"{BASE_URL}/{endpoint}",
            auth=(self.api_key, ""),
            params=params
        )
        response.raise_for_status()
        return response.json()

    def get_all(self, resource, params=None):
        rule = ENDPOINT_RULES.get(resource)
        print('self',resource, params,self.api_key)
        if not rule:
            raise ValueError(f"No rule defined for {resource}")

        endpoint = resource

        #  SINGLETON → gọi 1 lần
        if rule["type"] == "singleton":
            print(f"[SINGLETON] Fetching {resource}")
            return self.get(endpoint)

        #  LIST → pagination
        all_data = []
        starting_after = None

        while True:
            query = dict(params or {})

            # chỉ add limit nếu endpoint support
            if rule.get("limit"):
                query["limit"] = 20

            if starting_after:
                query["starting_after"] = starting_after

            response = self.get(endpoint, params=query)

            data = response.get("data", [])
            print(f"Fetched {len(data)} | has_more={response.get('has_more')}")

            if not data:
                break

            all_data.extend(data)

            if not rule.get("pagination") or not response.get("has_more"):
                break

            starting_after = data[-1].get("id")

        return all_data