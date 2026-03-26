"""Async OpenF1 API client for all 18 endpoints."""

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

NON_SESSION_ENDPOINTS = {"meetings", "championship_drivers", "championship_teams"}


async def fetch_endpoint(
    client: httpx.AsyncClient,
    endpoint: str,
    params: dict[str, Any],
) -> list[dict]:
    url = f"{config.OPENF1_BASE_URL}/{endpoint}"
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.json()


async def fetch_all_race_sessions() -> list[dict]:
    """Fetch all available race sessions from OpenF1."""
    transport = httpx.AsyncHTTPTransport(retries=3)
    async with httpx.AsyncClient(timeout=30.0, transport=transport) as client:
        response = await client.get(
            f"{config.OPENF1_BASE_URL}/sessions",
            params={"session_type": "Race"},
        )
        response.raise_for_status()
        return response.json()


async def fetch_session_data(session_key: int) -> dict[str, list[dict]]:
    """Fetch all endpoints for a given session key sequentially with retries."""
    params = {"session_key": session_key}
    data = {}

    transport = httpx.AsyncHTTPTransport(retries=3)
    async with httpx.AsyncClient(timeout=30.0, transport=transport) as client:
        for endpoint in ENDPOINTS:
            try:
                result = await fetch_endpoint(
                    client,
                    endpoint,
                    params if endpoint not in NON_SESSION_ENDPOINTS else {},
                )
                data[endpoint] = result
                print(f"  {endpoint}: {len(result)} rows")
            except Exception as e:
                print(f"  Warning: failed to fetch {endpoint}: {e}")
                data[endpoint] = []

    return data
