from datetime import datetime, timedelta
from airflow.models import Variable


def get_lookback_days(run_config=None):
    # 1. fallback config
    if run_config and run_config.get("lookback_days"):
        return run_config["lookback_days"]
    # 2. ưu tiên Airflow Variable
    try:
        return int(Variable.get("stripe_lookback_days"))
    except Exception:
        pass
    # 3. default
    return 3


def resolve_execution_date(context=None):
    if context:
        # ưu tiên chuẩn Airflow
        return context.get("data_interval_end") or context["logical_date"]
    return datetime.utcnow()


def build_time_window(context=None, run_config=None):
    execution_date = resolve_execution_date(context)
    lookback_days = get_lookback_days(run_config)

    end = int(execution_date.timestamp())
    start = int((execution_date - timedelta(days=lookback_days)).timestamp())

    return {
        "start": start,
        "end": end,
        "execution_date": execution_date.isoformat()
    }