"""Write raw OpenF1 data to Snowflake RAW schema via snowflake-connector-python."""

import json
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

import config
from client import NON_SESSION_ENDPOINTS, BULK_ENDPOINTS


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
    """Return session keys that are missing data in any of the given endpoints.

    A session is considered incomplete if it exists in SESSIONS but has 0 rows
    in any of the specified endpoint tables.
    """
    try:
        conn = _get_connection()
        cur = conn.cursor()

        # Get all known session keys
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
                        break  # no need to check other endpoints for this session
                except Exception:
                    # table doesn't exist yet — session is incomplete
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


def has_session_data(cur, table: str, session_key: int) -> bool:
    """Return True if this session_key already has data in the table."""
    cur.execute(
        f"SELECT COUNT(*) FROM {table.upper()} "
        f"WHERE raw_data:session_key::integer = {session_key}"
    )
    return cur.fetchone()[0] > 0


def load_rows(conn, table: str, rows: list[dict]) -> int:
    """Insert rows as VARIANT using write_pandas. Returns number of rows inserted."""
    if not rows:
        return 0

    df = pd.DataFrame({"RAW_DATA": [json.dumps(row) for row in rows]})
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table.upper()} (raw_data VARIANT, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())")

    tmp_table = f"{table.upper()}_TMP"
    cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {tmp_table} (raw_data STRING)")
    write_pandas(conn, df, tmp_table, auto_create_table=False, overwrite=True)
    cur.execute(f"INSERT INTO {table.upper()} (raw_data) SELECT PARSE_JSON(raw_data) FROM {tmp_table}")
    cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    return len(rows)


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
    """Create tables and load all endpoint data into Snowflake RAW schema.

    rows=None  → API confirmed no data (404/422), skip silently — do not retry
    rows=[]    → fetch failed with a retryable error, skip — will retry next run
    rows=[...] → load into Snowflake
    """
    session_key = None
    if "sessions" in data and data["sessions"]:
        session_key = data["sessions"][0].get("session_key")

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
            elif session_key and has_session_data(cur, endpoint, session_key):
                print(f"  Skipping {endpoint} — already loaded for session {session_key}")
                continue

            print(f"  Loading {endpoint}...")
            inserted = load_rows(conn, endpoint, rows)
            print(f"  Inserted {inserted} rows into RAW.{endpoint.upper()}")
        conn.commit()
    finally:
        conn.close()
