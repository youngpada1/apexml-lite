"""Async OpenF1 API client for all 18 endpoints."""

import asyncio
from typing import Any

import httpx

from ingestion.src import config

ENDPOINTS = [
    "car_data",
    "drivers",
    "intervals",
    "laps",
    "location",
    "meetings",
    "overtakes",
    "pit",
    "position",
    "race_control",
    "sessions",
    "session_result",
    "starting_grid",
    "stints",
    "team_radio",
    "weather",
    "championship_drivers",
    "championship_teams",
]


async def fetch_endpoint(
    client: httpx.AsyncClient,
    endpoint: str,
    params: dict[str, Any],
) -> list[dict]:
    url = f"{config.OPENF1_BASE_URL}/{endpoint}"
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.json()


async def fetch_session_data(session_key: int) -> dict[str, list[dict]]:
    """Fetch all endpoints for a given session key concurrently."""
    params = {"session_key": session_key}

    # These endpoints don't filter by session_key
    non_session_endpoints = {"meetings", "championship_drivers", "championship_teams"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = {
            endpoint: fetch_endpoint(
                client,
                endpoint,
                params if endpoint not in non_session_endpoints else {},
            )
            for endpoint in ENDPOINTS
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    data = {}
    for endpoint, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            print(f"Warning: failed to fetch {endpoint}: {result}")
            data[endpoint] = []
        else:
            data[endpoint] = result
            print(f"Fetched {len(result)} rows from {endpoint}")

    return data
