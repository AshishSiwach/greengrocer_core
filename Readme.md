# GreenGrocer Core: The Lean Analytics Stack

## Overview
A "Small Data" engineering project using Docker, Postgres, and Python to manage inventory and sales for a grocery chain.

## Architecture
1. **Ingest:** Python + OpenFoodFacts API
2. **Store:** PostgreSQL (Tuned for OLAP)
3. **Transform:** dbt Core
4. **Orchestrate:** Dagster

## Setup
1. `pip install -r requirements.txt`
2. `python scripts/fetch_reference_data.py`