"""Load starting grid for all sessions into Snowflake RAW."""

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import snowflake.connector
from cryptography.hazmat.primitives import serialization

import config

TABLE = "APEXML_DB.RAW.STARTING_GRID"


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


def fetch_starting_grid() -> list[dict]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(f"{config.OPENF1_BASE_URL}/starting_grid")
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, list) else []


def already_loaded(cur, session_key: int) -> bool:
    cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE RAW_DATA:session_key::integer = {session_key}")
    return cur.fetchone()[0] > 0


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] Starting load_starting_grid")

    rows = fetch_starting_grid()
    print(f"Fetched {len(rows)} rows from API")

    conn = get_connection()
    cur = conn.cursor()

    loaded = 0
    skipped = 0
    errors = []

    # Group by session_key to check per session
    sessions_seen = set()
    for r in rows:
        sk = r.get("session_key")
        try:
            if sk not in sessions_seen:
                if already_loaded(cur, sk):
                    sessions_seen.add(sk)

            if sk in sessions_seen and already_loaded(cur, sk) and loaded == 0:
                skipped += 1
                continue

            cur.execute(
                f"INSERT INTO {TABLE} (RAW_DATA, LOADED_AT) SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP()",
                (json.dumps(r),),
            )
            loaded += 1
        except Exception as e:
            ts_err = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"ERROR [{ts_err}] session_key={sk}: {e}")
            errors.append((sk, str(e)))

    conn.commit()
    cur.close()
    conn.close()

    ts_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{ts_end}] Done. Loaded {loaded} records.")
    print(f"Skipped (already loaded): {skipped}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for sk, err in errors:
            print(f"  session_key={sk}: {err}")


if __name__ == "__main__":
    main()
