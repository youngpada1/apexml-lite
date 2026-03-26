"""Orchestrator — fetch session data from OpenF1 and load into Snowflake RAW."""

import argparse
import asyncio

import httpx

from ingestion.src.client import fetch_all_race_sessions, fetch_session_data
from ingestion.src.loader import get_loaded_session_keys, load_all


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest OpenF1 session data into Snowflake")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--session-key",
        type=int,
        help="Load a specific session key",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Load all available race sessions (skips already loaded ones)",
    )
    group.add_argument(
        "--new",
        action="store_true",
        help="Load only sessions not yet in Snowflake (used by scheduled CI)",
    )
    return parser.parse_args()


async def run_session(session_key: int) -> None:
    print(f"\n--- Session {session_key} ---")
    data = await fetch_session_data(session_key)
    total_rows = sum(len(rows) for rows in data.values())
    print(f"Fetched {total_rows} total rows")
    load_all(data)
    print(f"Session {session_key} loaded.")


async def run_all(skip_existing: bool = False) -> None:
    print("Fetching all available race sessions from OpenF1...")
    sessions = await fetch_all_race_sessions()
    all_keys = [s["session_key"] for s in sessions if s.get("session_key")]
    print(f"Found {len(all_keys)} race sessions")

    if skip_existing:
        with httpx.Client(timeout=30.0) as client:
            loaded_keys = get_loaded_session_keys(client)
        new_keys = [k for k in all_keys if k not in loaded_keys]
        print(f"Skipping {len(loaded_keys)} already loaded — loading {len(new_keys)} new sessions")
        keys_to_load = new_keys
    else:
        keys_to_load = all_keys

    for session_key in keys_to_load:
        await run_session(session_key)

    print(f"\nDone. Loaded {len(keys_to_load)} sessions.")


if __name__ == "__main__":
    args = parse_args()

    if args.session_key:
        asyncio.run(run_session(args.session_key))
    elif args.all:
        asyncio.run(run_all(skip_existing=False))
    elif args.new:
        asyncio.run(run_all(skip_existing=True))
