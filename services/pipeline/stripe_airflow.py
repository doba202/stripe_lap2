"""Stripe raw load: Python callables for Airflow (keeps DAG file thin)."""

from __future__ import annotations

import json
from datetime import datetime

from airflow.models import Variable

from services.bigquery.repository import delete_all, delete_by_time_range, get_table_name, insert_raw
from services.common.config import (
    DATASET,
    ENDPOINT_RULES,
    PROJECT_ID,
    STRIPE_ACCOUNTS,
    TABLE_CONFIG,
    STRIPE_RESOURCES
)
from services.common.time_window import build_time_window
from services.stripe.client import StripeClient


def get_run_mode(context, default="daily"):
    dag_run = context.get("dag_run")

    if not dag_run:
        print(f"[MODE] No dag_run → default={default}")
        return default

    conf = dag_run.conf or {}
    mode = conf.get("run_mode", default)
    print(f"[MODE] {mode}")
    return mode


def get_open_id_filter():
    raw_value = Variable.get("STRIPE_OPEN_ID_FILTER", default_var="").strip()
    if not raw_value:
        return set()

    try:
        parsed = json.loads(raw_value)
        if isinstance(parsed, list):
            return {str(item).strip() for item in parsed if str(item).strip()}
    except json.JSONDecodeError:
        pass

    return {value.strip() for value in raw_value.split(",") if value.strip()}


def transform_record(resource, record, open_id):
    config = TABLE_CONFIG.get(resource)
    if not config:
        return None
    print("record", record)
    schema = config["schema"]
    mode = config["mode"]

    now = datetime.utcnow().isoformat()
    row = {}

    for field in schema:
        field_name = field["name"]
        field_mode = field.get("mode", "NULLABLE")

        if field_name == "call_at":
            row[field_name] = now
            continue

        if field_name == "open_id":
            row[field_name] = open_id
            continue

        if field_name == "created_at":
            created_ts = record.get("created")
            if created_ts:
                row[field_name] = datetime.utcfromtimestamp(created_ts).isoformat()
            else:
                row[field_name] = None
            continue

        if mode == "json" and field_name == "data_json":
            row[field_name] = record
            continue

        value = record.get(field_name)
        if field_mode == "REPEATED":
            row[field_name] = value if value is not None else []
        else:
            row[field_name] = value

    return row


def get_child_resources(resource):
    rule = ENDPOINT_RULES.get(resource, {})
    return rule.get("children", [])


def delete_resource_by_mode(resource, mode, context, open_id_filter=None):
    config = TABLE_CONFIG[resource]
    table_name = get_table_name(resource)

    if mode == "init":
        delete_all(table_name, open_ids=open_id_filter)
        return

    if mode == "daily":
        time_window = build_time_window(context)
        print("time_window", time_window["start"], time_window["end"])
        delete_by_time_range(
            table_name,
            time_window["start"],
            time_window["end"],
            time_field=config["time_field"],
            open_ids=open_id_filter
        )


def start(**context):
    print("Start pipeline...")


def delete_task(resource, **context):
    mode = get_run_mode(context)
    open_id_filter = get_open_id_filter()
    delete_resource_by_mode(resource, mode, context, open_id_filter)
    for child_resource in get_child_resources(resource):
        print(f"Delete child resource={child_resource} with parent resource={resource}")
        delete_resource_by_mode(child_resource, mode, context, open_id_filter)


def load_data(resource, **context):
    mode = get_run_mode(context)
    open_id_filter = get_open_id_filter()
    if open_id_filter:
        print(f"Filter enabled by Airflow Variable STRIPE_OPEN_ID_FILTER: {sorted(open_id_filter)}")
    else:
        print("No open_id filter configured; process all accounts.")

    for account in STRIPE_ACCOUNTS:
        open_id = account.get("open_id")
        if open_id_filter and open_id not in open_id_filter:
            print(f"Skip account open_id={open_id} by STRIPE_OPEN_ID_FILTER")
            continue

        client = StripeClient(account["api_key"])
        print(f"Loading {resource}...")
        data = client.fetch(resource, mode, context=context)
        if not data:
            print(f"No data for {resource}")
            continue

        if isinstance(data, dict):
            data = [data]
        print(f"Fetched {len(data)} records")

        transformed = []
        for record in data:
            row = transform_record(resource, record, open_id)
            if row:
                transformed.append(row)
        if not transformed:
            print(f"No transformed data for {resource}")
            continue

        print(f"Transformed {len(transformed)} records")
        print("transformed", transformed)
        insert_raw(
            table_name=f"{PROJECT_ID}.{DATASET}.stripe_raw_{resource}",
            records=transformed,
            schema=TABLE_CONFIG[resource]["schema"],
        )
        print(f"Inserted {len(transformed)} records into {resource}")

        for child_resource in get_child_resources(resource):
            child_rule = ENDPOINT_RULES.get(child_resource, {})
            parent_id_field = child_rule.get("parent_id_field", "id")
            parent_param = child_rule.get("parent_param")

            if not parent_param:
                print(f"Skip child resource={child_resource}: missing parent_param in rule")
                continue

            parent_ids = {record.get(parent_id_field) for record in data if record.get(parent_id_field)}
            if not parent_ids:
                print(f"No parent ids for child resource={child_resource}")
                continue

            child_records = []
            for parent_id in sorted(parent_ids):
                fetched = client.fetch(
                    child_resource,
                    mode,
                    context=context,
                    params={parent_param: parent_id},
                )
                if not fetched:
                    continue
                if isinstance(fetched, dict):
                    fetched = [fetched]
                child_records.extend(fetched)

            if not child_records:
                print(f"No records for child resource={child_resource}")
                continue

            transformed_child_records = []
            for child_record in child_records:
                row = transform_record(child_resource, child_record, open_id)
                if row:
                    transformed_child_records.append(row)

            if not transformed_child_records:
                print(f"No transformed records for child resource={child_resource}")
                continue

            insert_raw(
                table_name=f"{PROJECT_ID}.{DATASET}.stripe_raw_{child_resource}",
                records=transformed_child_records,
                schema=TABLE_CONFIG[child_resource]["schema"],
            )
            print(f"Inserted {len(transformed_child_records)} records into {child_resource}")
