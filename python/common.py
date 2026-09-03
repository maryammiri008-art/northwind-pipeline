import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv

DEFAULT_PROJECT_HOME = Path.home() / "de_final_project"
PROJECT_HOME = Path(os.getenv("PROJECT_HOME", DEFAULT_PROJECT_HOME))
load_dotenv(PROJECT_HOME / ".env")

def norm_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())

def json_serializable(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return None
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def canonical_payload(payload):
    if payload is None:
        return {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    result = {}
    for k, v in payload.items():
        if isinstance(v, (bytes, bytearray, memoryview)):
            continue
        elif isinstance(v, (date, datetime)):
            result[norm_name(str(k))] = v.isoformat()
        else:
            result[norm_name(str(k))] = v
    return result

def resolve_tables(cursor):
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        """
    )
    actual = {norm_name(row[0]): row[0] for row in cursor.fetchall()}
    wanted = [
        "customers", "employees", "suppliers", "products",
        "categories", "orders", "orderdetails", "shippers"
    ]
    missing = [name for name in wanted if name not in actual]
    if missing:
        raise RuntimeError(
            "Northwind table(s) not found after normalized matching: "
            + ", ".join(missing)
        )
    return {name: actual[name] for name in wanted}

KEY_FIELDS = {
    "customers": ["customerid"],
    "employees": ["employeeid"],
    "suppliers": ["supplierid"],
    "products": ["productid"],
    "categories": ["categoryid"],
    "orders": ["orderid"],
    "orderdetails": ["orderid", "productid"],
    "shippers": ["shipperid"],
}

def build_row_key(table_name: str, payload: dict) -> str:
    table_name = norm_name(table_name)
    payload = canonical_payload(payload)
    fields = KEY_FIELDS.get(table_name)
    if not fields:
        raise KeyError(f"No key mapping defined for table: {table_name}")
    values = [str(payload.get(field, "")) for field in fields]
    if any(v == "" for v in values):
        raise ValueError(
            f"Could not build row key for {table_name}; expected fields: {fields}; payload={payload}"
        )
    return "|".join(values)

def env(name: str, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value
