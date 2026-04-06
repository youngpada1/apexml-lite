"""Check if there are new sessions in the OpenF1 API not yet loaded in Snowflake RAW.

Exits with code 0 if new sessions found (proceed with ingestion).
Exits with code 1 if no new sessions (skip ingestion).
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import snowflake.connector
from cryptography.hazmat.primitives import serialization

import config


def get_connection():
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


def fetch_api_session_keys() -> set[int]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(f"{config.OPENF1_BASE_URL}/sessions")
        response.raise_for_status()
        sessions = response.json()
    return {s["session_key"] for s in sessions if s.get("session_key")}


def fetch_loaded_session_keys(cur) -> set[int]:
    cur.execute("SELECT DISTINCT RAW_DATA:session_key::integer FROM APEXML_DB.RAW.SESSIONS")
    return {int(row[0]) for row in cur.fetchall() if row[0] is not None}


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] Checking for new sessions...")

    api_keys = fetch_api_session_keys()
    print(f"API has {len(api_keys)} sessions")

    conn = get_connection()
    cur = conn.cursor()
    loaded_keys = fetch_loaded_session_keys(cur)
    cur.close()
    conn.close()
    print(f"Snowflake RAW has {len(loaded_keys)} sessions")

    new_keys = api_keys - loaded_keys
    if new_keys:
        print(f"Found {len(new_keys)} new sessions: {sorted(new_keys)}")
        print("Proceeding with ingestion.")
        sys.exit(0)
    else:
        print("No new sessions found. Skipping ingestion.")
        sys.exit(1)


if __name__ == "__main__":
    main()
