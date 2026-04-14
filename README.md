# ApexML-Lite - F1 Race Analytics Platform

A Snowflake-native data engineering platform for Formula 1 race analytics using OpenF1 API, Snowflake Cortex AI, dbt, and Streamlit in Snowflake.

[![Python](https://img.shields.io/badge/Python-3.13+-blue.svg)](https://www.python.org/)
[![Terraform](https://img.shields.io/badge/Terraform-1.6+-purple.svg)](https://www.terraform.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit_in_Snowflake-red.svg)](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cortex_AI-29B5E8.svg)](https://www.snowflake.com/)

---

## Project Overview

ApexML-Lite is a fully Snowflake-native F1 analytics platform that:

- **Extracts** real-time Formula 1 data from the OpenF1 API using Python (uv + httpx)
- **Loads** data directly into Snowflake RAW schema (no intermediate storage)
- **Transforms** data using dbt (RAW → STAGING → PROD)
- **Predicts** lap time degradation and race outcomes with Snowflake Cortex FORECAST()
- **Chats** about F1 data via a Cortex COMPLETE() powered chatbot
- **Visualises** everything through Streamlit in Snowflake (no external hosting)

---

## Architecture

```
OpenF1 API (18 endpoints)
        │
        ↓ httpx (async)
┌───────────────────┐
│   ingestion/      │  Python (uv + httpx) → writes directly to Snowflake
└────────┬──────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────┐
│                  Snowflake (fully native)                │
├─────────────────────────────────────────────────────────┤
│  RAW schema    18 tables — one per OpenF1 endpoint       │
│    CAR_DATA · DRIVERS · INTERVALS · LAPS · LOCATION      │
│    MEETINGS · OVERTAKES · PIT · POSITION · RACE_CONTROL  │
│    SESSION_RESULT · SESSIONS · STARTING_GRID · STINTS    │
│    TEAM_RADIO · WEATHER · CHAMPIONSHIP_DRIVERS/TEAMS     │
├─────────────────────────────────────────────────────────┤
│  STAGING schema   18 views — clean types, renamed cols   │
│    stg_sessions · stg_laps · stg_stints · stg_pit        │
│    stg_drivers · stg_meetings · stg_position · ...       │
├─────────────────────────────────────────────────────────┤
│  PROD schema   dims + facts                              │
│    dims:  DIM_SESSIONS · DIM_MEETINGS · DIM_DRIVERS      │
│           DIM_CHAMPIONSHIP_DRIVERS · DIM_CHAMPIONSHIP_TEAMS │
│    facts: FCT_LAPS · FCT_STINTS · FCT_PIT_STOPS          │
│           FCT_SESSION_RESULTS · FCT_RACE_POSITIONS        │
│           FCT_INTERVALS · FCT_CAR_DATA · FCT_LOCATION     │
│           FCT_WEATHER · FCT_RACE_CONTROL · FCT_OVERTAKES  │
│           FCT_STARTING_GRID · FCT_TEAM_RADIO              │
├─────────────────────────────────────────────────────────┤
│  Cortex COMPLETE()   │  Predictions, forecasts, summaries, chart specs  │
│  Cortex Analyst      │  Natural language to SQL via semantic model      │
├─────────────────────────────────────────────────────────┤
│  Streamlit in Snowflake                                  │
│    Calendar: Race Calendar & Results                     │
│    Race:     Results, Positions, Strategy, Lap Times,    │
│              Track Dominance, Telemetry                  │
│    Chatbot:  Cortex COMPLETE() interface                 │
│    Forecast: Cortex FORECAST() viewer                    │
└─────────────────────────────────────────────────────────┘
         ↑
┌────────────────────┐
│   terraform/       │  Provisions all Snowflake resources
└────────────────────┘
         ↑
┌────────────────────┐
│ .github/workflows/ │  CI/CD: tf apply, ingest, dbt run
└────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.13, uv, httpx (async) |
| Data Warehouse | Snowflake |
| Transformation | dbt Core + dbt-snowflake |
| AI / ML | Snowflake Cortex AI (COMPLETE, FORECAST, Analyst) |
| UI | Streamlit in Snowflake |
| Infrastructure | Terraform (Snowflake provider) |
| CI/CD | GitHub Actions |
| Package Manager | uv |

---

## Project Structure

```
apexml-lite/
├── ingestion/              # Python ELT — OpenF1 API → Snowflake RAW
│   ├── src/
│   │   ├── client.py       # Async OpenF1 API client (all 18 endpoints)
│   │   ├── loader.py       # Snowflake connector — MERGE into RAW schema
│   │   ├── main.py         # Orchestrator: fetch session → load all endpoints
│   │   └── config.py       # Env-based config
│   ├── pyproject.toml
│   └── .env.example
├── dbt/                    # Transformations: RAW → STAGING → PROD
│   ├── models/
│   │   ├── staging/        # One view per RAW table (clean types, renamed cols)
│   │   │   ├── stg_sessions.sql
│   │   │   ├── stg_meetings.sql
│   │   │   ├── stg_drivers.sql
│   │   │   ├── stg_laps.sql
│   │   │   ├── stg_stints.sql
│   │   │   ├── stg_pit.sql
│   │   │   ├── stg_position.sql
│   │   │   ├── stg_intervals.sql
│   │   │   ├── stg_car_data.sql
│   │   │   ├── stg_location.sql
│   │   │   ├── stg_weather.sql
│   │   │   ├── stg_race_control.sql
│   │   │   ├── stg_session_result.sql
│   │   │   ├── stg_starting_grid.sql
│   │   │   ├── stg_team_radio.sql
│   │   │   ├── stg_overtakes.sql
│   │   │   ├── stg_championship_drivers.sql
│   │   │   └── stg_championship_teams.sql
│   │   └── marts/          # Dims and facts in PROD schema
│   │       ├── dim_sessions.sql
│   │       ├── dim_meetings.sql
│   │       ├── dim_drivers.sql
│   │       ├── dim_championship_drivers.sql
│   │       ├── dim_championship_teams.sql
│   │       ├── fct_laps.sql
│   │       ├── fct_stints.sql
│   │       ├── fct_pit_stops.sql
│   │       ├── fct_session_results.sql
│   │       ├── fct_race_positions.sql
│   │       ├── fct_intervals.sql
│   │       ├── fct_car_data.sql
│   │       ├── fct_location.sql
│   │       ├── fct_weather.sql
│   │       ├── fct_race_control.sql
│   │       ├── fct_overtakes.sql
│   │       ├── fct_starting_grid.sql
│   │       └── fct_team_radio.sql
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
├── streamlit/              # Streamlit in Snowflake app
│   ├── app.py              # Thin router (~30 lines)
│   ├── tabs/
│   │   ├── calendar.py     # Race Calendar & Results grid
│   │   ├── race.py         # Race header + 6 sub-tabs
│   │   ├── results.py      # Tab 1: Session results table
│   │   ├── positions.py    # Tab 2: Race positions chart
│   │   ├── strategy.py     # Tab 3: Tyre strategy Gantt
│   │   ├── lap_times.py    # Tab 4: Lap times chart
│   │   ├── track_dominance.py  # Tab 5: Track dominance map
│   │   ├── telemetry.py    # Tab 6: Driver telemetry
│   │   ├── chatbot.py      # Cortex COMPLETE() interface
│   │   └── forecast.py     # Cortex FORECAST() viewer
│   ├── utils/
│   │   ├── connection.py   # Snowpark session (local + SiS)
│   │   └── colors.py       # Team & tyre colour constants
│   └── pyproject.toml
├── snowflake/
│   └── cortex/
│       ├── semantic_model.yaml   # Cortex Analyst semantic layer
│       ├── chatbot_prompt.sql
│       └── forecast_setup.sql
├── terraform/              # All Snowflake infra as code
│   ├── main.tf             # Warehouse, database, schemas
│   ├── roles.tf            # LOADER, TRANSFORMER, REPORTER roles
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── scripts/
│   └── generate_readme.py  # This script
├── .github/
│   └── workflows/
│       ├── readme.yml      # Auto-generate README on push
│       ├── terraform.yml   # tf validate + apply on push to main
│       ├── ingest.yml      # Scheduled OpenF1 ingestion
│       └── dbt.yml         # dbt run + dbt test after ingest
└── pyproject.toml
```

---

## Dependencies

Managed with **uv** per module (see each module's `pyproject.toml`).

| Module | Key Dependencies |
|---|---|
| `ingestion/` | httpx, snowflake-connector-python, cryptography |
| `dbt/` | dbt-core, dbt-snowflake |
| `streamlit/` | streamlit, snowflake-snowpark-python, pandas, plotly, altair |

---

## Quick Start

### Prerequisites
- Python 3.13+
- uv package manager
- Snowflake account
- Terraform 1.6+

### Setup

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone <your-repo-url>
cd apexml-lite
uv sync

# Provision Snowflake infrastructure
cd terraform
cp terraform.tfvars.example terraform.tfvars  # fill in your credentials
terraform init
terraform apply

# Run ingestion (fetch a session from OpenF1)
cd ../ingestion
uv run python src/main.py --session-key 9158

# Run dbt transformations
cd ../dbt
dbt run && dbt test

# Deploy Streamlit app via Snowflake UI or snowsql
```

---

## CI/CD

| Workflow | Trigger | Action |
|---|---|---|
| `readme.yml` | Push to `pyproject.toml` / `uv.lock` | Auto-generate README |
| `terraform.yml` | Push to `main` | `terraform validate` + `apply` |
| `ingest.yml` | Schedule / manual | Pull OpenF1 data → Snowflake RAW |
| `dbt.yml` | After ingest | `dbt run` + `dbt test` |

---

## OpenF1 API Endpoints Used

| Endpoint | Data |
|---|---|
| `car_data` | Speed, throttle, brake, RPM, DRS, gear @ 3.7Hz |
| `laps` | Sector times, speed trap, pit out laps |
| `stints` | Tyre compound, tyre age |
| `pit` | Pit stop durations |
| `position` | Driver positions throughout session |
| `intervals` | Gap to leader and car ahead |
| `weather` | Track/air temp, humidity, rainfall |
| `race_control` | Flags, safety car, incidents |
| `session_result` | Final standings |
| `drivers` | Name, team, number |
| `sessions` | Session metadata |
| `meetings` | GP weekend info |

---

## Author

**Flavia Ferreira**
- GitHub: [@youngpada1](https://github.com/youngpada1)

---

_README auto-generated via GitHub Actions_
