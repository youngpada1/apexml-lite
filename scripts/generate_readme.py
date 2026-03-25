#!/usr/bin/env python3
"""Auto-generate README.md from pyproject.toml and installed packages."""

import subprocess
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def get_direct_dependencies():
    """Extract direct dependencies from pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    return data.get("project", {}).get("dependencies", [])


def get_all_installed_packages():
    """Get all installed packages from uv pip list."""
    try:
        result = subprocess.run(
            ["uv", "pip", "list", "--format=columns"],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout:
            lines = result.stdout.strip().split("\n")
            package_lines = [line for line in lines[2:] if line.strip()]
            packages = []
            for line in package_lines:
                parts = line.split()
                if len(parts) >= 2:
                    packages.append((parts[0], parts[1]))
            return packages
    except Exception as e:
        print(f"Warning: Could not get package list: {e}")
        return []


def generate_readme():
    """Generate README.md content."""
    direct_deps = get_direct_dependencies()
    all_packages = get_all_installed_packages()

    readme_content = f"""# ApexML-Lite - F1 Race Analytics Platform

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
- **Transforms** data using dbt (RAW → STAGING → ANALYTICS)
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
│  RAW schema       │  Landing zone — raw JSON rows        │
│  STAGING schema   │  dbt: clean types, rename columns    │
│  ANALYTICS schema │  dbt: facts, dims, KPIs              │
├─────────────────────────────────────────────────────────┤
│  Cortex FORECAST()   │  Lap degradation & race outcomes  │
│  Cortex COMPLETE()   │  F1 chatbot (natural language)    │
│  Cortex Analyst      │  Semantic model over ANALYTICS    │
├─────────────────────────────────────────────────────────┤
│  Streamlit in Snowflake                                  │
│    Tab 1: Dashboard   (telemetry & KPIs)                 │
│    Tab 2: Chatbot     (Cortex COMPLETE)                  │
│    Tab 3: Forecast    (Cortex FORECAST agentic AI)       │
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
│   │   ├── loader.py       # Snowflake connector — writes to RAW schema
│   │   ├── main.py         # Orchestrator: fetch session → load all endpoints
│   │   └── config.py       # Env-based config
│   ├── pyproject.toml
│   └── .env.example
├── dbt/                    # Transformations: RAW → STAGING → ANALYTICS
│   ├── models/
│   │   ├── staging/        # One model per RAW table
│   │   └── marts/          # fct_laps, fct_pit_stops, dim_drivers, dim_sessions
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
├── streamlit/              # Streamlit in Snowflake app
│   ├── app.py
│   ├── pages/
│   │   ├── dashboard.py
│   │   ├── chatbot.py      # Cortex COMPLETE() interface
│   │   └── forecast.py     # Cortex FORECAST() viewer
│   └── environment.yml
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

Managed with **uv** (see [pyproject.toml](pyproject.toml))

### Direct Dependencies

```
{chr(10).join(direct_deps) if direct_deps else "None yet — see ingestion/pyproject.toml for ingestion deps"}
```

### All Installed Packages ({len(all_packages)} total)

<details>
<summary>View all packages</summary>

```
{chr(10).join(f"{name:<40} {version}" for name, version in all_packages) if all_packages else "Run uv sync to populate"}
```

</details>

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
"""

    readme_path = Path(__file__).parent.parent / "README.md"
    with open(readme_path, "w") as f:
        f.write(readme_content)

    print("README.md generated successfully")
    print(f"Direct dependencies: {len(direct_deps)}")
    print(f"Total packages: {len(all_packages)}")


if __name__ == "__main__":
    generate_readme()
