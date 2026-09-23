# TradeCorp Platform

**Overview**

TradeCorp Data Platform is an end-to-end Data Engineering project designed to simulate a modern enterprise analytics environment. The platform ingests transactional trade data, processes it through a multi-layered data architecture, and exposes curated analytical datasets for business reporting.

The project demonstrates industry-standard data engineering practices including orchestration, distributed processing, containerisation, data modelling, and cloud storage integration.

**Project Goals**

The primary objectives of this project are:

- Build a production-inspired data platform
- Implement scalable ETL pipelines using PySpark
- Orchestrate workflows using Apache Airflow
- Store raw and processed datasets following Medallion Architecture
- Integrate with Azure Data Lake Storage
- Create analytics-ready datasets
- Demonstrate Docker-based deployment
- Apply software engineering best practices to data pipelines

**Architecture**

![image.png](image.png)

**Technology Stack**

| **Category** | **Technology** |
| --- | --- |
| Language | Python 3.12 |
| Workflow Orchestration | Apache Airflow |
| Data Processing | Apache Spark / PySpark |
| Database | PostgreSQL |
| Containerisation | Docker |
| Container Management | Docker Compose |
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Data Modelling | Star Schema |
| Development Environment | VS Code |
| Version Control | Git & GitHub |
| Operating System | Ubuntu WSL |

**Project Structure**

![image.png](image%201.png)

**Data Architecture**

The project follows the Medallion Architecture pattern.

**Bronze Layer**

Purpose:

- Preserve original source data
- Enable data lineage
- Provide replay capability

Characteristics:

- Minimal transformation
- Schema enforcement
- Source metadata retained

Example:

bronze_trades

bronze_customers

bronze_products

**Silver Layer**

Purpose:

- Clean data
- Standardise formats
- Apply business rules

Typical Processing:

- Remove duplicates
- Handle missing values
- Standardise dates
- Data validation
- Customer enrichment

Example:

silver_trades

<aside>
💡

silver_customers

silver_products

**Gold Layer**

</aside>

Purpose:

- Business consumption
- KPI calculation
- Analytical reporting

Example datasets:

<aside>
💡

fact_sales

fact_trades

dim_customer

dim_product

dim_date

</aside>

**Data Pipeline Workflow**

**Step 1**

Ingest raw trade files

<aside>
💡

CSV

JSON

Parquet

</aside>

Files are collected from source systems and stored within the raw landing zone.

**Step 2**

Load to Bronze

Tasks:

<aside>
💡

- Read source files
- Validate schema
- Add ingestion timestamp
- Store immutable records
</aside>

**Step 3**

Transform to Silver

Tasks:

<aside>
💡

- Clean data
- Deduplicate records
- Perform joins
- Enforce business rules
</aside>

Example:

df = df.dropDuplicates()

df = df.filter(col("trade_amount") > 0)

**Step 4**

Build Gold Models

Tasks:

- Generate dimensions
- Generate facts
- Compute KPIs

Examples:

<aside>
💡

Revenue

Trade Volume

Customer Activity

Regional Performance

</aside>

**Step 5**

Publish Analytics Layer

The Gold Layer serves as the source for:

<aside>
💡

- Dashboards
- Reporting
- Ad-hoc analysis
- Data science workloads
</aside>

**Airflow Orchestration**

Apache Airflow coordinates all stages of the platform.

**Pipeline DAG**

![image.png](image%202.png)

**Airflow Components**

**Scheduler**

Schedules DAG execution.

**Webserver**

Provides monitoring and administration interface.

**Metadata Database**

Stores:

<aside>
💡

- DAG runs
- Task states
- Logs
- Connections
- Variables
</aside>

Implemented using PostgreSQL.

**Docker Deployment**

The entire platform runs using Docker Compose.

Services:

postgres

airflow-init

airflow-webserver

airflow-scheduler

spark-master

spark-worker

Start the environment:

docker compose up -d

Stop the environment:

docker compose down

View logs:

docker compose logs -f

**Spark Processing**

PySpark performs distributed processing for all transformation workloads.

Example:

spark.read.csv()

spark.read.parquet()

``

Typical transformations include:

- Aggregations
- Joins
- Window functions
- Data quality validation
- Schema enforcement

**Azure Data Lake Storage Integration**

The platform supports Azure Data Lake Storage Gen2.

Benefits:

<aside>
💡

- Scalable object storage
- Separation of storage and compute
- Enterprise-grade security
- Cost-efficient analytics
</aside>

Connection is configured using Spark Hadoop properties.

Example:

spark.conf.set(

"fs.azure.account.key.account.dfs.core.windows.net",

storage_key

)

**Data Quality Framework**

- [ ]  Quality checks are executed before publishing datasets.
- [ ]  Validation rules include:
- [ ]  **Completeness**
- [ ]  No critical null values
- [ ]  **Uniqueness**
- [ ]  No duplicate primary keys
- [ ]  **Validity**
- [ ]  Trade amount > 0
- [ ]  **Referential Integrity**
- [ ]  Fact records must match dimensions

**Monitoring**

Airflow provides monitoring capabilities including:

- [ ]  DAG status
- [ ]  Task duration
- [ ]  Task retries
- [ ]  Historical runs
- [ ]  Error tracking

Health checks are configured for:

PostgreSQL

Airflow Scheduler

Airflow Webserver

Spark Services

**Security Considerations**

Sensitive configuration is externalised using environment variables.

Examples:

```jsx
POSTGRES_USER=
```

```jsx
POSTGRES_PASSWORD=
```

```jsx
AZURE_STORAGE_KEY=
```

```jsx
AIRFLOW_UID=
```

Secrets should never be committed to source control.

**Future Improvements**

Planned enhancements:

**Infrastructure as Code**

- Terraform
- Azure Resource Manager

**CI/CD**

- GitHub Actions
- Automated testing
- Automated deployments

**Data Quality**

- Great Expectations
- Automated validation reports

**Storage**

- Delta Lake
- Apache Iceberg

**Streaming**

- Apache Kafka
- Spark Structured Streaming

**Data Warehouse**

- Azure Synapse Analytics
- Snowflake

**Key Skills Demonstrated**

This project showcases:

- Data Engineering Fundamentals
- ETL Design
- ELT Design
- Apache Airflow
- Apache Spark
- PySpark Development
- Docker
- PostgreSQL
- Data Modelling
- Data Quality Frameworks
- Cloud Storage Integration
- Workflow Automation
- Software Engineering Practices
- Git Version Control

**Learning Outcomes**

Through this project, I gained hands-on experience in:

1. Designing scalable data pipelines
2. Building orchestration workflows
3. Implementing distributed data processing
4. Working with cloud storage solutions
5. Managing containerised applications
6. Applying layered data architectures
7. Developing analytics-ready datasets
8. Troubleshooting production-style data platform issues

**Author**

**Tesfabirhan ("Tess")**

Aspiring Data Engineer focused on building modern cloud-native data platforms using Python, Spark, Airflow, Docker, PostgreSQL, and Azure technologies.

GitHub: [Tesfabirhan-WR (Tesfabirhan REDIE)](https://github.com/Tesfabirhan-WR)

LinkedIn: [Tesfabirhan REDIE | LinkedIn](https://www.linkedin.com/in/tesfabirhan-redie-85092765/)