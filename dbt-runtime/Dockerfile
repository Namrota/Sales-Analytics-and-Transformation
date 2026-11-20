FROM python:3.11-slim-bookworm

WORKDIR /workspace

# Install Git required by dbt
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Install dbt and DuckDB adapter
RUN pip install --no-cache-dir \
    "dbt-core==1.9.11" \
    "dbt-duckdb==1.9.6"

CMD ["dbt", "--version"]