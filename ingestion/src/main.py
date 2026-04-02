"""Orchestrator — fetch session data from OpenF1 and load into Snowflake RAW."""

import argparse
import time

from client import ENDPOINTS, fetch_all_race_sessions, fetch_session_data
from loader import get_incomplete_session_keys, get_loaded_session_keys, load_all


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
        help="Load all available race sessions",
    )
    group.add_argument(
        "--new",
        action="store_true",
        help="Load new sessions and backfill any missing endpoints in existing sessions (used by CI)",
    )
    parser.add_argument(
        "--endpoints",
        nargs="+",
        metavar="ENDPOINT",
        help=f"Only fetch these endpoints (default: all). Available: {', '.join(ENDPOINTS)}",
    )
    return parser.parse_args()


def run_session(session_key: int, endpoints: list[str] | None = None) -> None:
    print(f"\n--- Session {session_key} ---")
    data = fetch_session_data(session_key, endpoints=endpoints)
    total_rows = sum(len(rows) for rows in data.values())
    print(f"Fetched {total_rows} total rows")
    load_all(data)
    print(f"Session {session_key} loaded.")


def run_all(skip_existing: bool = False, endpoints: list[str] | None = None) -> None:
    print("Fetching all available race sessions from OpenF1...")
    sessions = fetch_all_race_sessions()
    all_keys = [s["session_key"] for s in sessions if s.get("session_key")]
    print(f"Found {len(all_keys)} race sessions")

    if skip_existing:
        # New sessions not yet in Snowflake at all
        loaded_keys = get_loaded_session_keys()
        new_keys = [k for k in all_keys if k not in loaded_keys]

        # Existing sessions that are missing one or more endpoints
        check_endpoints = endpoints if endpoints is not None else ENDPOINTS
        incomplete_keys = get_incomplete_session_keys(check_endpoints)
        # Only backfill existing sessions (not brand new ones — they'll be fully loaded anyway)
        backfill_keys = [k for k in incomplete_keys if k in loaded_keys]

        keys_to_load = new_keys + backfill_keys
        print(
            f"New sessions: {len(new_keys)} | "
            f"Existing sessions with missing endpoints: {len(backfill_keys)} | "
            f"Total to process: {len(keys_to_load)}"
        )
    else:
        keys_to_load = all_keys

    for i, session_key in enumerate(keys_to_load):
        run_session(session_key, endpoints=endpoints)
        if i < len(keys_to_load) - 1:
            time.sleep(5)

    print(f"\nDone. Loaded {len(keys_to_load)} sessions.")


if __name__ == "__main__":
    args = parse_args()
    endpoints = args.endpoints or None

    if args.session_key:
        run_session(args.session_key, endpoints=endpoints)
    elif args.all:
        run_all(skip_existing=False, endpoints=endpoints)
    elif args.new:
        run_all(skip_existing=True, endpoints=endpoints)
