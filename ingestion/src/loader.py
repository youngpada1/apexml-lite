"""Write raw OpenF1 data to Snowflake RAW schema via Snowflake SQL API."""

import json
import uuid

import httpx

from ingestion.src import config
from ingestion.src.auth import generate_jwt


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {generate_jwt()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }


def _execute_sql(client: httpx.Client, statement: str, bindings: dict | None = None) -> dict:
    body: dict = {
        "statement": statement,
        "warehouse": config.SNOWFLAKE_WAREHOUSE,
        "database": config.SNOWFLAKE_DATABASE,
        "schema": config.SNOWFLAKE_SCHEMA,
        "role": config.SNOWFLAKE_ROLE,
        "requestId": str(uuid.uuid4()),
    }
    if bindings:
        body["bindings"] = bindings

    response = client.post(
        config.SNOWFLAKE_SQL_API_URL,
        headers=_headers(),
        json=body,
    )
    response.raise_for_status()
    return response.json()


def ensure_table(client: httpx.Client, table: str) -> None:
    """Create RAW table if it doesn't exist — stores each row as a VARIANT."""
    _execute_sql(
        client,
        f"""
        CREATE TABLE IF NOT EXISTS {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{table.upper()} (
            raw_data VARIANT,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
    )


def load_rows(client: httpx.Client, table: str, rows: list[dict]) -> int:
    """Insert rows as VARIANT into a RAW table. Returns number of rows inserted."""
    if not rows:
        return 0

    table_upper = table.upper()
    inserted = 0

    # Insert in batches of 1000
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = ", ".join(
            f"(PARSE_JSON('{json.dumps(row).replace(chr(39), chr(39)+chr(39))}'))"
            for row in batch
        )
        _execute_sql(
            client,
            f"INSERT INTO {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{table_upper} (raw_data) VALUES {values}",
        )
        inserted += len(batch)

    return inserted


def get_loaded_session_keys(client: httpx.Client) -> set[int]:
    """Return set of session keys already loaded in Snowflake RAW.SESSIONS."""
    try:
        result = _execute_sql(
            client,
            f"SELECT DISTINCT raw_data:session_key::integer AS session_key "
            f"FROM {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.SESSIONS",
        )
        rows = result.get("data", [])
        return {int(row[0]) for row in rows if row[0] is not None}
    except Exception:
        return set()


def load_all(data: dict[str, list[dict]]) -> None:
    """Create tables and load all endpoint data into Snowflake RAW schema."""
    with httpx.Client(timeout=60.0) as client:
        for endpoint, rows in data.items():
            print(f"Loading {endpoint}...")
            ensure_table(client, endpoint)
            inserted = load_rows(client, endpoint, rows)
            print(f"  Inserted {inserted} rows into RAW.{endpoint.upper()}")
