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

def normalize_to_day(dt):
    return datetime(dt.year, dt.month, dt.day)

def build_time_window(context=None, run_config=None):
    execution_date = resolve_execution_date(context)
    lookback_days = get_lookback_days(run_config)

    #  normalize về 00:00:00
    execution_day = normalize_to_day(execution_date)

    start = int((execution_day - timedelta(days=lookback_days)).timestamp())
    end = int((execution_day + timedelta(days=1)).timestamp()) - 1

    return {
        "start": start,
        "end": end,
        "execution_date": execution_date.isoformat()
    }