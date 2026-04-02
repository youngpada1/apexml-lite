"""Sync OpenF1 API client for all 18 endpoints."""

import time
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


def fetch_endpoint(
    client: httpx.Client,
    endpoint: str,
    params: dict[str, Any],
) -> list[dict]:
    url = f"{config.OPENF1_BASE_URL}/{endpoint}"
    response = client.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        return []
    return data


def fetch_all_race_sessions() -> list[dict]:
    """Fetch all available race sessions from OpenF1."""
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        response = client.get(
            f"{config.OPENF1_BASE_URL}/sessions",
            params={"session_type": "Race"},
        )
        response.raise_for_status()
        return response.json()


def fetch_session_data(
    session_key: int,
    endpoints: list[str] | None = None,
) -> dict[str, list[dict]]:
    """Fetch endpoints for a given session key sequentially with retries.

    Args:
        session_key: OpenF1 session key.
        endpoints: Subset of endpoints to fetch. Defaults to all ENDPOINTS.
    """
    params = {"session_key": session_key}
    data = {}
    targets = endpoints if endpoints is not None else ENDPOINTS

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        for endpoint in targets:
            try:
                result = fetch_endpoint(
                    client,
                    endpoint,
                    params if endpoint not in NON_SESSION_ENDPOINTS else {},
                )
                data[endpoint] = result
                print(f"  {endpoint}: {len(result)} rows")
            except Exception as e:
                print(f"  Warning: failed to fetch {endpoint}: {e}")
                data[endpoint] = []
            time.sleep(1)

    return data
