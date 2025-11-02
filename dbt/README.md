# dbt for Data Engineering Project

## Setup
- Copy `profiles.yml.example` to your local `~/.dbt/profiles.yml` and set credentials via env vars or edit directly.
- Install dbt and ClickHouse adapter:
  ```powershell
  pip install dbt-core dbt-clickhouse clickhouse-connect
  ```
- Run models:
  ```powershell
  dbt run
  dbt test
  ```

## Medallion Layers
- Bronze: loaded by Airflow DAGs into ClickHouse tables (see `get_static_data.py` and `get_live_data.py`).
- Silver: cleaning/deduplication models in `models/silver/`.
- Gold: fact/dimension models in `models/gold/`.
