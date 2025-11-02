# Data Engineering project 2025

Our project for the Data Engineering course at UniTartu CS.

Team-members: Martin Aasmäe, René Piik, Markus Ilves, Kaidi Tootmaa

## Research of the Swedish transportation system time delays

Data is gathered from: <https://www.trafiklab.se/api/netex-datasets/netex-regional/>

## Running the project

Before any code can be run, you must create a copy of the `.env.template` file named `.env` and add the api keys for both static (NETEX) and live (SIRI) data.

Once the API keys are in place, just

1. run `docker compose build` to install the required python packages,
2. run `docker compose up -d`.

NB! If for some reason the `compose up` command does not start services correctly (this can be monitored more easily by running `docker compose up` without the `-d` flag), try removing the `:z` from the end of lines 14 and 30 of `compose.yml`.
Those were added by René due to specifics of running docker on Fedora Linux, but the fix might mess up the services on other platforms.

## Reading Airflow task logs

Log files from Airflow for any task, for example `get_live_data`, can be read with the following command:

```bash
cat logs/dag_id=get_live_data/run_id=scheduled__<datetime>/task_id=get_live_data/attempt=1.log
```

A list of all attempts for a given task run can be seen when running this command:

```bash
ls logs/dag_id=get_live_data/run_id=scheduled__<datetime>/task_id=get_live_data
```

Substitute `<datetime>` for any valid value, for example `2025-10-27T21:54:00+00:00`.


## Screenshots or visuals of Airflow and dbt DAGs

<figure>
  <img width="1899" height="528" alt="image" src="https://github.com/user-attachments/assets/3b16028c-281e-4969-83f2-17c42d896111"/>
  <caption>A list of all our Airflow DAGs.</caption>
</figure>

<figure>
  <img width="1897" height="881" alt="image" src="https://github.com/user-attachments/assets/0d08e5e5-b458-415c-84e0-2e5034b43453" />
  <caption>Our static data DAG with a task related to dbt.</caption>
</figure>

## dbt setup for our project

### Setup
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

### Medallion Layers
- Bronze: loaded by Airflow DAGs into ClickHouse tables (see `get_static_data.py` and `get_live_data.py`).
- Silver: cleaning/deduplication models in `models/silver/`.
- Gold: fact/dimension models in `models/gold/`.


