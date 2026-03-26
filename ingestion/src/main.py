"""Orchestrator — fetch session data from OpenF1 and load into Snowflake RAW."""

import argparse
import asyncio

from ingestion.src.client import fetch_session_data
from ingestion.src.loader import load_all


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest OpenF1 session data into Snowflake")
    parser.add_argument(
        "--session-key",
        type=int,
        required=True,
        help="OpenF1 session key (e.g. 9158 for a specific race session)",
    )
    return parser.parse_args()


async def run(session_key: int) -> None:
    print(f"Fetching data for session {session_key}...")
    data = await fetch_session_data(session_key)

    total_rows = sum(len(rows) for rows in data.values())
    print(f"Fetched {total_rows} total rows across {len(data)} endpoints")

    print("Loading into Snowflake RAW...")
    load_all(data)
    print("Done.")


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.session_key))
