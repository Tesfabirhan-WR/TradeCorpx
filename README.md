# TradeCorp Data Platform

TradeCorp is an end-to-end data engineering project that simulates a modern enterprise analytics platform. It ingests trade and sales-related datasets, validates and stores them in a Bronze layer, transforms them into a curated Silver dataset, and prepares the foundation for downstream analytics, reporting, and business intelligence workloads.

The project brings together Apache Airflow, Apache Spark, Docker, PostgreSQL, and Azure Blob Storage in a single reproducible environment designed to model a production-style ETL workflow.

## Overview

This repository demonstrates:

- Data ingestion from cloud/object storage
- ETL orchestration with Airflow
- Distributed processing with PySpark
- Layered storage using a medallion architecture
- Data cleaning and enrichment for analytical consumption
- Docker-based local development and deployment

The platform is structured around a simple but realistic flow:

1. Download raw files from Azure Blob Storage
2. Validate source data availability
3. Write raw data as immutable Bronze Parquet tables
4. Apply transformations and business logic
5. Produce a Silver dataset enriched with customer and currency metadata
6. Provide a ready-to-use foundation for Gold-layer analytics

## Architecture

The project follows a medallion architecture:

- Bronze: raw, immutable source data preserved in Parquet format
- Silver: cleaned, standardized, and enriched data for analytics use
- Gold: analytical models, dimensions, facts, and KPI-oriented datasets

At a high level:

```text
Azure Blob Storage
        │
        ▼
   [Airflow DAG]
        │
   raw validation
        │
   Spark bronze ingestion
        │
   Bronze Parquet tables
        │
   PySpark silver transformation
        │
   orders_enriched (Silver)
        │
   Gold / BI / reporting layer
```

The main orchestration DAG is defined in [dags/tradecorp_pipeline.py](dags/tradecorp_pipeline.py), and the transformation logic lives primarily in [src/pipeline.py](src/pipeline.py) and the supporting ETL utilities under [src/](src/).

## Technology Stack

| Area | Technology |
| --- | --- |
| Language | Python 3.12 |
| Workflow orchestration | Apache Airflow |
| Data processing | Apache Spark / PySpark |
| Storage | Azure Blob Storage / local Parquet |
| Database | PostgreSQL |
| Containerization | Docker / Docker Compose |
| Environment | VS Code + WSL/Linux |
| Version control | Git / GitHub |

## Repository Structure

```text
TradeCorpx/
├── dags/                          # Airflow DAG definitions and validation logic
│   └── tradecorp_pipeline.py
├── src/                          # ETL scripts, transformations, readers, and writers
│   ├── pipeline.py
│   ├── reader.py
│   ├── transformer.py
│   ├── writer.py
│   ├── utils.py
│   ├── CurrencyEnrichment.py
│   └── CountryCurrencyUploader.py
├── tests/                       # Project tests and validation scripts
├── notebooks/                  # Notebook-based experimentation and analysis
├── config/                     # Project configuration files
├── data/                       # Runtime data directories (created during execution)
├── docker-compose.yaml         # Local service orchestration
├── Dockerfile.airflow          # Apache Airflow image build
├── Dockerfile.spark            # Spark container image build
├── requirements-airflow.txt    # Airflow environment dependencies
├── requirements-spark.txt      # Spark environment dependencies
├── TradeCorp_readme.md         # Additional project documentation
├── .gitignore
├── README.md
└── ...
```

## Data Pipeline Flow

### 1. Raw extraction
The workflow begins with the raw data pipeline task that downloads source files from Azure Blob Storage to a local raw directory. The extraction step is implemented in [src/reader.py](src/reader.py).

### 2. Bronze layer
The raw datasets are converted into Parquet and stored in immutable Bronze tables. This preserves the original source records and supports lineage and replay use cases.

### 3. Silver layer
The Silver transformation reads Bronze tables and builds an enriched dataset, including:

- customer-level context
- order details and relationships
- product and category joins
- employee and shipper context
- currency enrichment by country

This is handled primarily by [src/transformer.py](src/transformer.py), [src/CurrencyEnrichment.py](src/CurrencyEnrichment.py), and [src/pipeline.py](src/pipeline.py).

### 4. Gold layer
The Gold layer is the business-facing layer intended for dashboards, KPI analysis, analytical models, and reporting workloads.

## Prerequisites

Before running the project locally, ensure you have:

- Docker and Docker Compose installed
- Python 3.12+ recommended
- Access to Azure Blob Storage credentials for the raw ingestion step
- A configured `.env` file for environment variables used by the services

## Environment Variables

The project expects Azure-related credentials and runtime configuration values such as:

- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_KEY`
- `CONTAINER_NAME`
- `CURRENCY_CSV_PATH`
- `BRONZE_PATH`
- `SILVER_PATH`

The ingestion step reads these values from the environment and will fail fast if required Azure settings are missing.

## Quick Start

From the repository root:

```bash
docker compose up --build -d
```

This starts the configured services, including:

- Spark workspace on port `8889`
- Airflow web UI on port `8081`
- PostgreSQL on port `5432`

## Airflow Access

After startup, open the Airflow UI in your browser:

- URL: http://localhost:8081
- Username: `admin`
- Password: `admin`

The DAG named `tradecorp_etl` orchestrates the full flow.

## Useful Commands

Start services:

```bash
docker compose up --build -d
```

Stop services:

```bash
docker compose down
```

View running services:

```bash
docker compose ps
```

Inspect logs:

```bash
docker compose logs -f airflow-scheduler
```

## Notes

This project is designed as a learning and demonstration platform. It is intentionally production-inspired rather than a full enterprise SaaS application. The codebase focuses on demonstrating clean ETL patterns, orchestration, distributed data processing, and realistic data platform architecture.

## Roadmap / Planned Extension

Potential future enhancements include:

- Gold layer star-schema datasets and materialized views
- Data quality checks and monitoring
- CI/CD automation and tests
- Postgres warehouse integration
- Dashboarding and BI reporting
- Cloud deployment automation for Azure

## License

This project is provided for educational and demonstration purposes. Check your organization or repository policy for any licensing restrictions before using it in production or sharing it externally.
