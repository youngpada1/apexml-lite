"""Load pit stops for all sessions into Snowflake RAW."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import snowflake.connector
from cryptography.hazmat.primitives import serialization

import config

TABLE = "APEXML_DB.RAW.PIT"


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


def fetch_all_sessions() -> list[dict]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(f"{config.OPENF1_BASE_URL}/sessions")
        response.raise_for_status()
        return response.json()


def fetch_pit(session_key: int) -> list[dict]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(
            f"{config.OPENF1_BASE_URL}/pit",
            params={"session_key": session_key},
        )
        if response.status_code in (404, 422):
            return []
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, list) else []


def already_loaded(cur, session_key: int) -> bool:
    cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE RAW_DATA:session_key::integer = {session_key}")
    return cur.fetchone()[0] > 0


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] Starting load_pit")

    sessions = fetch_all_sessions()
    session_keys = [s["session_key"] for s in sessions if s.get("session_key")]
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
            if already_loaded(cur, sk):
                print("already loaded, skipping.")
                skipped += 1
                continue

            rows = fetch_pit(sk)
            if not rows:
                print("no data, skipping.")
                skipped += 1
                time.sleep(0.5)
                continue

            for r in rows:
                cur.execute(
                    f"INSERT INTO {TABLE} (RAW_DATA, LOADED_AT) SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP()",
                    (json.dumps(r),),
                )
            conn.commit()
            loaded += len(rows)
            print(f"{len(rows)} records inserted.")
        except Exception as e:
            ts_err = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"ERROR [{ts_err}]: {e}")
            errors.append((sk, str(e)))

        time.sleep(1.5)

    cur.close()
    conn.close()

    ts_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{ts_end}] Done. Loaded {loaded} records across {total - skipped - len(errors)} sessions.")
    print(f"Skipped (already loaded or no data): {skipped}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for sk, err in errors:
            print(f"  session_key={sk}: {err}")


if __name__ == "__main__":
    main()
