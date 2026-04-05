"""Write raw OpenF1 data to Snowflake RAW schema via snowflake-connector-python."""

import json
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

import config
from client import NON_SESSION_ENDPOINTS, BULK_ENDPOINTS

# Natural dedup keys per endpoint — used for MERGE to prevent duplicate inserts.
# Fields must match exact JSON keys in the raw_data VARIANT.
MERGE_KEYS: dict[str, list[str]] = {
    "car_data":            ["session_key", "driver_number", "date"],
    "drivers":             ["session_key", "driver_number"],
    "intervals":           ["session_key", "driver_number", "date"],
    "laps":                ["session_key", "driver_number", "lap_number"],
    "location":            ["session_key", "driver_number", "date"],
    "meetings":            ["meeting_key"],
    "overtakes":           ["session_key", "overtaking_driver_number", "overtaken_driver_number", "date"],
    "pit":                 ["session_key", "driver_number", "lap_number"],
    "position":            ["session_key", "driver_number", "date"],
    "race_control":        ["session_key", "date", "message"],
    "sessions":            ["session_key"],
    "session_result":      ["session_key", "driver_number"],
    "starting_grid":       ["session_key", "driver_number"],
    "stints":              ["session_key", "driver_number", "stint_number"],
    "team_radio":          ["session_key", "driver_number", "date"],
    "weather":             ["session_key", "date"],
    "championship_drivers": ["driver_number", "position"],
    "championship_teams":  ["team_name", "position"],
}


def _get_connection():
    with open(Path(config.SNOWFLAKE_PRIVATE_KEY_PATH), "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account=config.SNOWFLAKE_ACCOUNT,
        user=config.SNOWFLAKE_USER,
        private_key=private_key_bytes,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        role=config.SNOWFLAKE_ROLE,
    )


def get_loaded_session_keys() -> set[int]:
    """Return set of session keys already loaded in Snowflake RAW.SESSIONS."""
    try:
        conn = _get_connection()
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT raw_data:session_key::integer "
            f"FROM {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.SESSIONS"
        )
        return {int(row[0]) for row in cur.fetchall() if row[0] is not None}
    except Exception:
        return set()
    finally:
        conn.close()


def get_incomplete_session_keys(endpoints: list[str]) -> set[int]:
    """Return session keys that are missing data in any of the given endpoints."""
    try:
        conn = _get_connection()
        cur = conn.cursor()

        cur.execute(
            f"SELECT DISTINCT raw_data:session_key::integer "
            f"FROM {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.SESSIONS"
        )
        all_keys = {int(row[0]) for row in cur.fetchall() if row[0] is not None}

        incomplete = set()
        for session_key in all_keys:
            for endpoint in endpoints:
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{endpoint.upper()} "
                        f"WHERE raw_data:session_key::integer = {session_key}"
                    )
                    count = cur.fetchone()[0]
                    if count == 0:
                        incomplete.add(session_key)
                        break
                except Exception:
                    incomplete.add(session_key)
                    break

        return incomplete
    except Exception:
        return set()
    finally:
        conn.close()


def ensure_table(cur, table: str) -> None:
    """Create RAW table if it doesn't exist."""
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table.upper()} "
        f"(raw_data VARIANT, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    )


def _build_merge_condition(endpoint: str) -> str:
    """Build the ON clause for MERGE using the natural key fields for this endpoint."""
    keys = MERGE_KEYS.get(endpoint, [])
    if not keys:
        return "1=0"  # no key defined — never match, always insert (safe fallback)

    conditions = []
    for key in keys:
        # Infer Snowflake cast type from field name
        if key in ("date", "recorded_at"):
            cast = "::timestamp_ntz"
        elif key in ("message", "team_name", "classified_position", "compound"):
            cast = "::string"
        else:
            cast = "::integer"
        conditions.append(
            f"target.raw_data:{key}{cast} = source.raw_data:{key}{cast}"
        )
    return " AND ".join(conditions)


def load_rows(conn, table: str, rows: list[dict]) -> int:
    """Upsert rows as VARIANT using MERGE on natural key. Returns number of rows inserted."""
    if not rows:
        return 0

    endpoint = table.lower()
    df = pd.DataFrame({"RAW_DATA": [json.dumps(row) for row in rows]})
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table.upper()} "
        f"(raw_data VARIANT, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    )

    tmp_table = f"{table.upper()}_TMP"
    cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {tmp_table} (raw_data STRING)")
    write_pandas(conn, df, tmp_table, auto_create_table=False, overwrite=True)

    merge_condition = _build_merge_condition(endpoint)
    cur.execute(f"""
        MERGE INTO {table.upper()} AS target
        USING (SELECT PARSE_JSON(raw_data) AS raw_data FROM {tmp_table}) AS source
        ON {merge_condition}
        WHEN NOT MATCHED THEN
            INSERT (raw_data, loaded_at) VALUES (source.raw_data, CURRENT_TIMESTAMP())
    """)

    inserted = cur.rowcount
    cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    return inserted


def load_bulk(data: dict[str, list[dict]]) -> None:
    """Truncate and reload bulk endpoints (e.g. starting_grid) — called once per run."""
    conn = _get_connection()
    try:
        cur = conn.cursor()
        for endpoint, rows in data.items():
            if endpoint not in BULK_ENDPOINTS:
                continue
            ensure_table(cur, endpoint)
            cur.execute(f"TRUNCATE TABLE {endpoint.upper()}")
            if not rows:
                print(f"  {endpoint}: 0 rows — skipping")
                continue
            inserted = load_rows(conn, endpoint, rows)
            print(f"  Inserted {inserted} rows into RAW.{endpoint.upper()}")
        conn.commit()
    finally:
        conn.close()


def load_all(data: dict[str, list[dict] | None]) -> None:
    """Create tables and upsert all endpoint data into Snowflake RAW schema.

    rows=None  → API confirmed no data (404/422), skip silently — do not retry
    rows=[]    → fetch failed with a retryable error, skip — will retry next run
    rows=[...] → MERGE into Snowflake (idempotent — no duplicates regardless of reruns)
    """
    conn = _get_connection()
    try:
        cur = conn.cursor()
        for endpoint, rows in data.items():
            ensure_table(cur, endpoint)

            if endpoint in NON_SESSION_ENDPOINTS:
                cur.execute(f"TRUNCATE TABLE {endpoint.upper()}")
            elif rows is None:
                print(f"  Skipping {endpoint} — no data available in API")
                continue
            elif not rows:
                print(f"  Skipping {endpoint} — fetch failed, will retry next run")
                continue

            print(f"  Loading {endpoint}...")
            inserted = load_rows(conn, endpoint, rows)
            print(f"  Inserted {inserted} new rows into RAW.{endpoint.upper()}")
        conn.commit()
    finally:
        conn.close()
