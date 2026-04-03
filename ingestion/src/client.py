"""Sync OpenF1 API client for all 18 endpoints."""

import time
from typing import Any

import httpx

import config

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

# Endpoints that must be fetched without session_key (API doesn't support it) and loaded once per run
BULK_ENDPOINTS = {"starting_grid"}

# Endpoints that must be fetched per driver — OpenF1 rejects session-level requests as too large
PER_DRIVER_ENDPOINTS = {"car_data", "location"}


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


def fetch_bulk_data() -> dict[str, list[dict]]:
    """Fetch all BULK_ENDPOINTS (no session filter) in one shot."""
    transport = httpx.HTTPTransport(retries=3)
    data = {}
    with httpx.Client(timeout=30.0, transport=transport) as client:
        for endpoint in BULK_ENDPOINTS:
            try:
                result = fetch_endpoint(client, endpoint, {})
                data[endpoint] = result
                print(f"  {endpoint}: {len(result)} rows")
            except Exception as e:
                print(f"  ERROR fetching bulk {endpoint}: {e}")
                data[endpoint] = []
            time.sleep(1)
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
) -> dict[str, list[dict] | None]:
    """Fetch endpoints for a given session key sequentially with retries.

    Args:
        session_key: OpenF1 session key.
        endpoints: Subset of endpoints to fetch. Defaults to all ENDPOINTS.

    Returns:
        Dict of endpoint -> rows. Value is:
          - list[dict]: rows fetched successfully (may be empty list)
          - None: API confirmed no data (404/422) — do not retry
    """
    params = {"session_key": session_key}
    data: dict[str, list[dict] | None] = {}
    targets = endpoints if endpoints is not None else ENDPOINTS

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        # Get driver numbers for this session (needed for per-driver endpoints)
        driver_numbers: list[int] = []
        if any(e in PER_DRIVER_ENDPOINTS for e in targets):
            try:
                drivers = fetch_endpoint(client, "drivers", params)
                driver_numbers = [d["driver_number"] for d in drivers if "driver_number" in d]
            except Exception as e:
                print(f"  WARNING: could not fetch driver list for session {session_key}: {e}")

        for endpoint in targets:
            try:
                if endpoint in PER_DRIVER_ENDPOINTS and driver_numbers:
                    # Fetch per driver and merge
                    all_rows: list[dict] = []
                    for driver_number in driver_numbers:
                        driver_params = {**params, "driver_number": driver_number}
                        try:
                            rows = fetch_endpoint(client, endpoint, driver_params)
                            all_rows.extend(rows)
                            time.sleep(2)
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code in (404, 422):
                                pass  # no data for this driver, skip silently
                            else:
                                print(f"  ERROR fetching {endpoint} driver {driver_number} — will retry next run: {e}")
                        except Exception as e:
                            print(f"  ERROR fetching {endpoint} driver {driver_number} — will retry next run: {e}")
                    data[endpoint] = all_rows
                    print(f"  {endpoint}: {len(all_rows)} rows ({len(driver_numbers)} drivers)")
                else:
                    result = fetch_endpoint(
                        client,
                        endpoint,
                        params if endpoint not in NON_SESSION_ENDPOINTS else {},
                    )
                    data[endpoint] = result
                    print(f"  {endpoint}: {len(result)} rows")
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (404, 422):
                    print(f"  No data available in API for {endpoint} (HTTP {e.response.status_code})")
                    data[endpoint] = None
                else:
                    print(f"  ERROR fetching {endpoint} — will retry next run: {e}")
                    data[endpoint] = []
            except Exception as e:
                print(f"  ERROR fetching {endpoint} — will retry next run: {e}")
                data[endpoint] = []
            time.sleep(1)

    return data
