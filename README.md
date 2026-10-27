# Airflow + dbt + DuckDB Data Engineering Pipeline

## Project Overview

This project demonstrates an end-to-end local data engineering workflow using Apache Airflow, dbt Core, DuckDB, Python, and Docker.

The pipeline transforms sample sales data, validates the transformation using dbt tests, and orchestrates the process through Apache Airflow.

The repository also includes BritMart analytics.

## Technology Stack

| Technology | Purpose |
|---|---|
| Apache Airflow 3.3.2 | Workflow orchestration and monitoring |
| dbt Core 1.11.16 | SQL transformation and data-quality testing |
| dbt-duckdb 1.11.1 | DuckDB database adapter |
| DuckDB | Analytical database |
| Python 3.13 | Data verification and pipeline execution |
| Docker Compose | Containerized development environment |
| Git and GitHub | Version control |

## Project Structure

```text
airflow-project/
├── airflow/
│   ├── dags/
│   │   └── sales_dbt_pipeline.py
│   └── dbt/
│       └── sales_dbt/
│           ├── models/
│           │   └── example_model.sql
│           ├── seeds/
│           │   └── sales.csv
│           ├── tests/
│           │   └── assert_positive_total.sql
│           ├── dbt_project.yml
│           └── profiles.yml
├── assignments/
│   └── airflow_dbt_duckdb/
│       ├── verify_duckdb.py
│       └── screenshots/
├── Dockerfile.airflow-dbt
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Pipeline Architecture

```text
Sample sales.csv
       |
       v
Airflow DAG: sales_dbt_pipeline
       |
       v
check_dbt
       |
       v
build_models_and_test
       |
       v
dbt SQL Transformation
       |
       v
DuckDB: main.example_model
       |
       v
dbt Data Quality Test
       |
       v
Python Verification
```


## Running and Validating the dbt Pipeline

The dbt project is located at:

`airflow/dbt/sales_dbt/`

The container accesses it through:

`/opt/airflow/dbt/sales_dbt/`

    ### 1. Verify the dbt connection

    ```bash
    docker compose exec \
    -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
    -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
    -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
    sleek-airflow \
    dbt debug --project-dir /opt/airflow/dbt/sales_dbt
    ```

    ### 2. Build the model and execute tests

    ```bash
    docker compose exec \
    -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
    -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
    -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
    sleek-airflow \
    dbt build \
    --project-dir /opt/airflow/dbt/sales_dbt \
    --select example_model
    ```

    This command creates the `main.example_model` table and runs its associated `assert_positive_total` data-quality test.

    ### 3. Execute the Airflow DAG

    Open `http://localhost:8080` and trigger `sales_dbt_pipeline`.

    The pipeline contains two tasks:

    - `check_dbt`: validates the dbt project and DuckDB connection.
    - `build_models_and_test`: executes the SQL transformation and its associated data-quality test.

    ### 4. Verify the DuckDB results

    From the repository root, execute:

    ```bash
    docker run --rm \
    --user "1000:0" \
    --entrypoint python \
    -v "$(pwd)/airflow:/opt/airflow:ro" \
    -v "$(pwd)/assignments:/opt/assignments:ro" \
    my-airflow:dbt \
    /opt/assignments/airflow_dbt_duckdb/verify_duckdb.py
    ```

    The verification script reads `sales.duckdb` and displays the four transformed sales records.

    The model produces the following results:

    | Customer | Amount | Estimated Tax | Total with Tax |
    |---|---:|---:|---:|
    | Alice | 500 | 90 | 590 |
    | Bob | 700 | 126 | 826 |
    | Charlie | 300 | 54 | 354 |
    | Diana | 900 | 162 | 1,062 |

    The database file is generated locally and excluded from Git.

    ### 5. Data Quality Validation

    The custom SQL test is located at:

    `airflow/dbt/sales_dbt/tests/assert_positive_total.sql`

    It identifies records where `total_with_tax` is less than or equal to zero.

    A successful test returns zero failing records.

    ### 6. dbt Documentation

    Generate documentation:

    ```bash
    docker compose exec \
    -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
    -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
    -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
    sleek-airflow \
    dbt docs generate \
    --project-dir /opt/airflow/dbt/sales_dbt
    ```

    The generated documentation can be served on port 8081 using `dbt docs serve`.


## SQL Transformation

The model reads sample sales data using DuckDB's `read_csv_auto()` and calculates:

- `estimated_tax = amount * 0.18`
- `total_with_tax = amount * 1.18`

The resulting dataset contains four sales records.

## Data Quality Test

The custom test `assert_positive_total.sql` identifies records where:

```sql
total_with_tax <= 0
```

The test passes when no records violate the condition.

## Setup

### 1. Clone the repository

```bash
git clone <YOUR_REPOSITORY_SSH_URL>
cd airflow-project
```

### 2. Configure the environment

```bash
cp .env.example .env
```

Update `.env` with your own configuration. Set `AIRFLOW_UID` to the UID of your local Linux/WSL user.

MySQL and Gmail credentials are used by additional example DAGs and are not required by the standalone DuckDB sales transformation.

### 3. Build and start the containers

```bash
docker compose up -d --build
```

### 4. Verify dbt

```bash
docker compose exec sleek-airflow \
  dbt debug \
  --project-dir /opt/airflow/dbt/sales_dbt \
  --profiles-dir /opt/airflow/dbt/sales_dbt
```

### 5. Execute the dbt model and test

```bash
docker compose exec sleek-airflow \
  dbt build \
  --project-dir /opt/airflow/dbt/sales_dbt \
  --profiles-dir /opt/airflow/dbt/sales_dbt \
  --select example_model
```

### 6. Trigger the Airflow DAG

Open `http://localhost:8080`.

Find `sales_dbt_pipeline` and trigger it manually.

The DAG performs two operations:

1. `check_dbt` — validates the database connection and project configuration.
2. `build_models_and_test` — runs the SQL transformation and the associated data-quality test.

### 7. Verify the DuckDB table

Run `verify_duckdb.py` in an environment that has DuckDB installed and can access the database path configured in the script.

## Expected Results

| Customer | Amount | Estimated tax | Total with tax |
|---|---:|---:|---:|
| Alice | 500 | 90 | 590 |
| Bob | 700 | 126 | 826 |
| Charlie | 300 | 54 | 354 |
| Diana | 900 | 162 | 1062 |

**Validation:** Four sales records are transformed, and the positive-total data-quality test passes.

## Documentation

dbt documentation can be generated using `dbt docs generate` and served on port 8081.

The documentation UI allows exploration of models, database objects, and lineage.

## Security and Version Control

The repository excludes:

- `.env` and credentials
- Local DuckDB database files
- Airflow metadata database files
- Runtime logs
- Python virtual environments
- Generated dbt artifacts

Only non-sensitive configuration templates, source code, sample data, and documentation should be committed.

## Future Improvements

- Introduce additional data-quality tests.
- Add automated validation for missing or duplicate order IDs.
- Expand the pipeline to process larger datasets.
- Include continuous integration for dbt tests.
