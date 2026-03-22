import requests
from airflow.models import Variable
from datetime import datetime,timedelta
from services.common.config import BASE_URL, ENDPOINT_RULES
from services.common.time_window import build_time_window

class StripeClient:
    def __init__(self, api_key=None):
        self.api_key = api_key

    def _get_lookback_days(self, mode_conf):
        # ưu tiên Airflow Variable
        try:

            return int(Variable.get("stripe_lookback_days"))
        except:
            pass

        # fallback config
        if mode_conf.get("lookback_days"):
            return mode_conf["lookback_days"]

        # default
        return 3

    def _get(self, endpoint, params=None):
        print('[REQUEST]', endpoint, params)
        response = requests.get(
            f"{BASE_URL}/{endpoint}",
            auth=(self.api_key, ""),
            params=params
        )
        response.raise_for_status()
        return response.json()

    def _fetch_by_rule(self, resource, params=None):
        rule = ENDPOINT_RULES.get(resource)

        if not rule:
            raise ValueError(f"No rule defined for {resource}")

        endpoint = resource

        # 👉 1. SINGLETON
        if rule["type"] == "singleton":
            print(f"[SINGLETON] {resource}")
            return self._get(endpoint, params=params)

        # 👉 2. LIST nhưng KHÔNG pagination
        if not rule.get("pagination"):
            print(f"[LIST NO PAGINATION] {resource}")
            response = self._get(endpoint, params=params)
            return response.get("data", [])

        # 👉 3. LIST + pagination
        print(f"[PAGINATED] {resource}")

        all_data = []
        starting_after = None

        while True:
            query = dict(params or {})

            #  limit từ config
            if rule.get("limit"):
                query["limit"] = rule.get("page_size", 50)

            if starting_after:
                query["starting_after"] = starting_after

            response = self._get(endpoint, params=query)

            data = response.get("data", [])
            has_more = response.get("has_more", False)

            print(f"[PAGE] {len(data)} records | has_more={has_more}")

            if not data:
                break

            all_data.extend(data)

            # 👉 stop condition
            if not has_more:
                break

            #  Stripe pagination cursor
            starting_after = data[-1].get("id")

            # 👉 safety (tránh loop vô hạn nếu API lỗi)
            if not starting_after:
                print("[WARN] Missing 'id' for pagination, stopping")
                break

        return all_data

    def fetch(self, resource, mode="daily", context=None, params=None):
        rule = ENDPOINT_RULES.get(resource)
        mode_conf = rule["modes"].get(mode)

        #  lấy time window từ util
        time_window = build_time_window(context, mode_conf)
        start = time_window["start"]
        end = time_window["end"]
        print('time_window',time_window)
        #  build params từ config
        config_params = mode_conf.get("params", {})
        final_params = {}

        for k, v in config_params.items():
            if isinstance(v, str):
                if "{start}" in v:
                    final_params[k] = start
                elif "{end}" in v:
                    final_params[k] = end
            else:
                final_params[k] = v

        if params:
            final_params.update(params)

        print("[FETCH PARAMS]", final_params)

        return self._fetch_by_rule(resource, params=final_params)