"""Load championship teams into Snowflake RAW."""

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import snowflake.connector
from cryptography.hazmat.primitives import serialization

import config

TABLE = "APEXML_DB.RAW.CHAMPIONSHIP_TEAMS"


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


def fetch_championship_teams() -> list[dict]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(f"{config.OPENF1_BASE_URL}/championship_teams")
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, list) else []


def already_loaded(cur, team_name: str, position) -> bool:
    if position is None:
        cur.execute(
            f"SELECT COUNT(*) FROM {TABLE} "
            f"WHERE RAW_DATA:team_name::string = %s "
            f"AND RAW_DATA:position IS NULL",
            (team_name,),
        )
    else:
        cur.execute(
            f"SELECT COUNT(*) FROM {TABLE} "
            f"WHERE RAW_DATA:team_name::string = %s "
            f"AND RAW_DATA:position::integer = {position}",
            (team_name,),
        )
    return cur.fetchone()[0] > 0


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] Starting load_championship_teams")

    rows = fetch_championship_teams()
    print(f"Fetched {len(rows)} rows from API")

    conn = get_connection()
    cur = conn.cursor()

    loaded = 0
    skipped = 0
    errors = []

    for r in rows:
        team_name = r.get("team_name")
        position = r.get("position")
        try:
            if already_loaded(cur, team_name, position):
                skipped += 1
                continue

            cur.execute(
                f"INSERT INTO {TABLE} (RAW_DATA, LOADED_AT) SELECT PARSE_JSON(%s), CURRENT_TIMESTAMP()",
                (json.dumps(r),),
            )
            loaded += 1
        except Exception as e:
            ts_err = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"ERROR [{ts_err}] team_name={team_name}: {e}")
            errors.append((team_name, str(e)))

    conn.commit()
    cur.close()
    conn.close()

    ts_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{ts_end}] Done. Loaded {loaded} records.")
    print(f"Skipped (already loaded): {skipped}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for tn, err in errors:
            print(f"  team_name={tn}: {err}")


if __name__ == "__main__":
    main()
