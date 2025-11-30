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

NB! Upon inital startup Airflow might fail to read from / write to the the logs folder. If this happens you might see an error message like the following:
```
airflow-init-1 | Unable to load the config, contains a configuration error. 
airflow-init-1 | Traceback (most recent call last): 
airflow-init-1 | File "/usr/local/lib/python3.11/pathlib.py", line 1116, in mkdir 
airflow-init-1 | os.mkdir(self, mode) 
airflow-init-1 | FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/logs/scheduler/2025-11-22' 
airflow-init-1 | 
airflow-init-1 | During handling of the above exception, another exception occurred: 
airflow-init-1 | 
airflow-init-1 | Traceback (most recent call last): 
airflow-init-1 | File "/usr/local/lib/python3.11/logging/config.py", line 573, in configure 
airflow-init-1 | handler = self.configure_handler(handlers[name]) 
airflow-init-1 | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 
airflow-init-1 | File "/usr/local/lib/python3.11/logging/config.py", line 757, in configure_handler 
airflow-init-1 | result = factory(**kwargs) 
airflow-init-1 | ^^^^^^^^^^^^^^^^^ 
airflow-init-1 | File "/home/airflow/.local/lib/python3.11/site-packages/airflow/utils/log/file_processor_handler.py", line 50, in __init__ 
airflow-init-1 | Path(self._get_log_directory()).mkdir(parents=True, exist_ok=True) 
airflow-init-1 | File "/usr/local/lib/python3.11/pathlib.py", line 1120, in mkdir 
airflow-init-1 | self.parent.mkdir(parents=True, exist_ok=True) 
airflow-init-1 | File "/usr/local/lib/python3.11/pathlib.py", line 1116, in mkdir 
airflow-init-1 | os.mkdir(self, mode) 
airflow-init-1 | PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'
```

The most important part there is the final row, which explicitly states that airflow does not have permissions inside the `logs` folder.
To fix the error, run `docker compose down` and then use the following command: `sudo chown -R 50000:50000 logs`.

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

