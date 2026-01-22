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

## Architecture Diagram

```mermaid
graph TD
    subgraph "Legacy/OLTP World (Python Simulation)"
        A[External API<br>OpenFoodFacts] -->|Fetch Metadata| B(Reference Data<br>JSON)
        C[Chaos Engine<br>Python Scripts] -->|Generate| D(Raw Transactions<br>CSV Files)
        E[Inventory Sim<br>Python Scripts] -->|Generate| F(Delivery Logs<br>CSV Files)
    end

    subgraph "The Handshake (Ingestion)"
        D -->|Copy/Ingest| G[Landing Zone<br>Postgres (Raw Schema)]
        F -->|Copy/Ingest| G
    end

    subgraph "The Warehouse (OLAP)"
        G -->|dbt Build| H[Bronze Layer<br>Cleaning & Casting]
        H -->|dbt Build| I[Silver Layer<br>Joins & Logic]
        I -->|dbt Build| J[Gold Layer<br>Business Aggregates]
    end

    subgraph "Value Layer"
        J -->|Query| K[Reporting Dashboard<br>Streamlit/PgAdmin]
    end
```