"""One-off script to load race_control for all sessions into Snowflake RAW."""

import json
import time
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


def fetch_all_session_keys() -> list[int]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(f"{config.OPENF1_BASE_URL}/sessions")
        response.raise_for_status()
        sessions = response.json()
    return [s["session_key"] for s in sessions if s.get("session_key")]


def fetch_race_control(session_key: int) -> list[dict]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(
            f"{config.OPENF1_BASE_URL}/race_control",
            params={"session_key": session_key},
        )
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, list) else []


def main():
    print("Fetching all session keys from OpenF1...")
    session_keys = fetch_all_session_keys()
    print(f"Found {len(session_keys)} sessions")

    conn = get_connection()
    cur = conn.cursor()

    total = len(session_keys)
    loaded = 0
    skipped = 0
    errors = []

    for i, sk in enumerate(session_keys, 1):
        print(f"[{i}/{total}] session_key={sk}...", end=" ", flush=True)
        try:
            records = fetch_race_control(sk)
            if not records:
                print("no data, skipping.")
                skipped += 1
                time.sleep(0.5)
                continue

            for r in records:
                cur.execute(
                    "INSERT INTO APEXML_DB.RAW.RACE_CONTROL (RAW_DATA, LOADED_AT) "
                    "SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP()",
                    (json.dumps(r),),
                )
            loaded += len(records)
            print(f"{len(records)} records inserted.")
        except Exception as e:
            print(f"ERROR: {e}")
            errors.append((sk, str(e)))

        time.sleep(0.5)

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone. Loaded {loaded} records across {total - skipped - len(errors)} sessions.")
    print(f"Skipped (no API data): {skipped}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for sk, err in errors:
            print(f"  session_key={sk}: {err}")


if __name__ == "__main__":
    main()
