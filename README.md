# Data Engineering project 2025

Our project for the Data Engineering course at UniTartu CS.

Team-members: Martin Aasmäe, René Piik, Markus Ilves, Kaidi Tootmaa

## Research of the Swedish transportation system time delays

This project implements a data pipeline and predictive operational model to integrate real-time delay information with geographical stop data. The objective is to **minimize network bottlenecks, optimize scheduling, and substantially improve network punctuality**, thereby increasing customer satisfaction.

#### 1. Business Brief

* **Objective:** Build a predictive operational model by integrating real-time delay data with detailed geographical stop information to minimize bottlenecks and optimize scheduling.
* **Stakeholders:** The primary beneficiary is the Swedish rail operator **Örebro**. The pipeline uses the **Netex standard** for easy scalability to other Swedish and international rail systems.
* **Key Metrics (KPIs):**
    * **Punctuality by journey:** Percentage of stops where a train arrived within a specific window (e.g., on time, in 1-min span, in 2-min span).
    * **Punctuality by stops (bottlenecks):** Punctuality focused on specific bottleneck locations.
    * **Average system delay:** Mean delay (in minutes) across the entire system.
* **Business Questions:** The model aims to determine the average system delay, identify lines and segments most prone to delays, and pinpoint recurrent delays that could be amended via timetable changes.

#### 2. Datasets

All datasets are sourced from the **Trafiklab NeTEx regional API**.

* **SIRI Live Data (Real-Time):** Provides dynamic operational updates.
* **NeTEx Regional Static Data (Geographical):** Contains static stop information like `Name`, `StopPlaceType`, `Centroid_Long`, and `Centroid_Lat`.
* **Line-Specific XML Files:** Detailed information on routes, line names, and directions for specific lines.

---

## Setting up the project

#### Getting the data 

An API-key is required for running the project.
For this:
* Create an account in Trafiklab and log in.
* Create a new Sandbox project on the page <https://www.trafiklab.se/api/netex-datasets/netex-regional/>
* Create keys for:
    * NeTEx Regional Static data
    * SIRI

#### Set up itself

1. Before any code can be run, you must create a copy of the `.env.template` file named `.env` and add the API-keys for both static (NETEX) and live (SIRI) data as shown in the template.
2. Also copy `~/dbt/profiles.yml.example` to your local `~/dbt/profiles.yml` and set credentials via env vars or edit directly.
3. Make sure your Docker Engine is running and run `docker compose build` to install the required python packages.
4. Run `docker compose up -d`.
5. (If needed) Install dbt and ClickHouse adapter:
     ```powershell
     pip install dbt-core dbt-clickhouse clickhouse-connect
     ```
6. Run models:
     ```powershell
     dbt run
     dbt test
     ```

NB! If for some reason the `compose up` command does not start services correctly (this can be monitored more easily by running `docker compose up` without the `-d` flag), try removing the `:z` from the end of lines 14 and 30 of `compose.yml`.
Those were added by René due to specifics of running docker on Fedora Linux, but the fix might mess up the services on other platforms (there shouldn't be a problem with Windows).

##### Apache Airflow
Apache Iceberg is set up automatically by composing the container.
UI is accessible at: http://localhost:8081/
If you have added the API-keys to .env file, then please trigger the two DAGs visible to start the data ingestion pipeline.

##### Clickhouse
If the DAGs have run successfully you can see the data through Clickhouse.
UI is accessible at: http://localhost:8123/
You can run basic SQL like "SHOW TABLES;" etc to see the created data.

To create the two roles run the following commands from console:
* Creating the roles:
   * ```powershell
     cat sql/clickhouse_roles.sql | docker exec -i dataengineering_project-dbt-clickhouse-1 clickhouse-client
     ```
* Running the checks:
   * ```powershell
     cat sql/clickhouse_roles_check.sql | docker exec -i dataengineering_project-dbt-clickhouse-1 clickhouse-client
     ```

##### Apache Iceberg
Apache Iceberg is set up automatically by composing the container.
UI is accessbile at: http://localhost:9101/

##### OpenMetadata
NB! OpenMetadata additions are under branch OpenMetadata, where Iceberg is not implemented (as running Iceberg and OpenMetadata together in the same containers worked only for short periods of times as it crashed our Docker Engines).
Meaning our main branch does not involve OpenMetadata right now (only Clickhouse additions and Iceberg).

UI is accessible at: http://localhost:8585/ 
For testing the connection run the following SQL in Clickhouse:
* ```SQL
   CREATE ROLE role_openmetadata;
   CREATE USER service_openmetadata IDENTIFIED WITH sha256_password BY 'TrafiklabProject123';
   GRANT role_openmetadata TO service_openmetadata;
   GRANT SELECT, SHOW ON system.* to role_openmetadata;
   GRANT SELECT ON default.* TO role_openmetadata;
  ```
* Then create a Clickhouse service with the following information:
   * User: service_openmetadata
   * Password: TrafiklabProject123
   * Host and port: clickhouse:8123

##### Apache Superset
Not implemented as of now.

---
## About our data

#### Data architecture
<figure>
  <img width="1444" height="502" alt="image" src="https://github.com/kaidittm/dataengineering_project/blob/main/illustrations/data_architecture.jpg" />
</figure>

#### Data model
Fact Table: 
* Grain: One row per arrival or departure event at a stop point for a specific journey.
* Key Columns: ServiceJourneyID (FK), QuayID(FK), DateID (FK)
* Columns: AimedArrivalTime, ActualArrivalTime, AimedDepartureTime, ActualDepartureTime

Dimension tables:
* DimServiceJourney – ServiceJourneyID, LineName, RouteName, TransportMode, DirectionType, ValidFrom, ValidTo columns from the first table. Contains descriptive information about the specific journey and the route it belongs to.
* DimQuay – Contains QuayID, which is referenced in live data, and StopPlaceID, which references static metadata about stops.
* DimStopPoint – Contains detailed descriptive information about the geographical stop place (e.g., name, location, municipality).
* DimDate – The date component of the timestamp columns. Allows for analysis based on calendar components (day, week, month, year, holiday flags).


<div align="center">
  <figure>
    <img width="509" height="428" alt="image" src="https://github.com/kaidittm/dataengineering_project/blob/main/illustrations/star_schema.png" />
  </figure>
</div>


Slowly changing dimensions:
* Quay – type 2 (for example a quay could be removed from a station, which is crucial information when analysing data, also stop-points can be changed so it is necessary that the quay references the correct stop)
* StopPoint – type 2 (for example a bus-stop could be moved temporarily or even permanently because of construction)
* ServiceJourney – type 2 (for example the line name or route name could be changed in time)
* Date – type 0 (we assume that the calendar is a static attribute)

#### Data dictionary

This model consists of one Fact table (`Events`) tracking operational times and four Dimension tables (`DimServiceJourney`, `DimQuay`, `DimStopPoint`, `DimDate`) providing descriptive context.

##### 1. Events (Fact Table)

*The central table capturing the scheduled and actual arrival/departure times for a single journey at a single stop.*

| Column Name | Description | Data Type | Key Type |
| :--- | :--- | :--- | :--- |
| **EventID** | Primary key for the event record. | Integer | PK |
| **ServiceJourneyID** | Link to the `DimServiceJourney` table. | Integer | FK |
| **QuayID** | Link to the `DimQuay` table. | Integer | FK |
| **DateID** | Link to the `DimDate` table. | Integer | FK |
| **AimedArrivalTime** | The scheduled arrival time in the timetable. | Time | |
| **ActualArrivalTime** | The recorded time of actual arrival. | Time | |
| **AimedDepartureTime** | The scheduled departure time in the timetable. | Time | |
| **ActualDepartureTime** | The recorded time of actual departure. | Time | |

---

##### 2. DimServiceJourney

*Gathers information about a specific journey (vehicle, route, and time period).*

| Column Name | Description | Data Type | Key Type |
| :--- | :--- | :--- | :--- |
| **ServiceJourneyID** | Primary key for the service journey. | Integer | PK |
| **LineName** | The name of the line (e.g., "Line 51"). | Categorical | |
| **RouteName** | The name of the specific route (e.g., "Norra Bråten"). | Categorical | |
| **TransportMode** | The type of transportation (e.g., "bus"). | Categorical | |
| **DirectionType** | The direction of the journey (e.g., "outbound"). | Categorical | |
| **ValidFrom** | The date the validity of this row begins (SCD Type 2 start date). | Date | |
| **ValidTo** | The date the validity of this row ends (SCD Type 2 end date). | Date | |

---

##### 3. DimQuay

*Links the operational Quay ID (the specific physical stop position) to the larger StopPoint/Station. Uses Slowly Changing Dimension (SCD).*

| Column Name | Description | Data Type | Key Type |
| :--- | :--- | :--- | :--- |
| **QuayID** | Primary key for the quay/platform. | Integer | PK |
| **StopPlaceID** | Link to the `DimStopPoint` table. | Integer | FK |
| **ValidFrom** | The date the validity of this row begins. | Date | |
| **ValidTo** | The date the validity of this row ends. | Date | |

---

##### 4. DimStopPoint

*Gathers geographical and general information about the stop or station. Uses Slowly Changing Dimension (SCD).*

| Column Name | Description | Data Type | Key Type |
| :--- | :--- | :--- | :--- |
| **StopPointID** | Primary key for the stop/station. | Integer | PK |
| **Name** | The full name of the stop/station. | Categorical | |
| **ShortName** | The official shortened version of the name. | Categorical | |
| **Centroid_Long** | The longitude of the midpoint of the stop. | Numerical | |
| **Centroid_Lat** | The latitude of the midpoint of the stop. | Numerical | |
| **StopPlaceType** | Mode of transportation using this stop (e.g., "busStation"). | Categorical | |
| **ValidFrom** | The date the validity of this row begins. | Date | |
| **ValidTo** | The date the validity of this row ends. | Date | |

---

##### 5. DimDate

*A classic Time Dimension providing contextual information about the day of the event.*

| Column Name | Description | Data Type | Key Type |
| :--- | :--- | :--- | :--- |
| **DateID** | Primary key for the date record. | Integer | PK |
| **Day** | The day of the month. | Numerical | |
| **Weekday** | The weekday (e.g., "Monday"). | Categorical | |
| **Month** | The month (1-12). | Numerical | |
| **Year** | The year. | Numerical | |
| **IsHoliday** | Whether the day is a holiday (True/False). | Boolean | |

---

## Our data flow

#### Medallion Layers
* Bronze: loaded by Airflow DAGs into ClickHouse tables (see `get_static_data.py` and `get_live_data.py`).
* Silver: cleaning/deduplication models in `models/silver/`.
* Gold: fact/dimension models in `models/gold/`.

#### Reading Airflow task logs

Log files from Airflow for any task, for example `get_live_data`, can be seen from the Airflow user interface (<http://localhost:8081/>) or read with the following command:
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

<figure>
  <img width="1899" height="528" alt="image" src="https://github.com/kaidittm/dataengineering_project/blob/main/illustrations/dbt_schema.jpg"/>
  <caption>Our DBT diagram.</caption>
</figure>



