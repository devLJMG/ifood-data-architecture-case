# iFood Data Architecture Case

## Overview

This project implements a Data Architecture and Data Engineering solution for ingesting, transforming, modeling and analyzing NYC Yellow Taxi trip data from January to May 2023.

The solution was developed as part of the iFood technical case for a Data Architecture position. It demonstrates the use of PySpark, Parquet, Data Lake layers, SQL/PySpark analysis and an analytical dashboard using Streamlit.

## Objective

The main goal of this project is to:

* Ingest NYC Yellow Taxi trip data into a Data Lake structure.
* Preserve raw files in a Landing Zone.
* Transform and standardize the data using PySpark.
* Create a Consumption Zone for analytical usage.
* Answer the required business questions using SQL and PySpark.
* Provide a dashboard view for data exploration, process visibility and data quality checks.

## Dataset

The data used in this project comes from the official NYC Taxi & Limousine Commission trip record dataset.

Period processed:

* January 2023
* February 2023
* March 2023
* April 2023
* May 2023

Data source:

```text
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

Files used:

```text
yellow_tripdata_2023-01.parquet
yellow_tripdata_2023-02.parquet
yellow_tripdata_2023-03.parquet
yellow_tripdata_2023-04.parquet
yellow_tripdata_2023-05.parquet
```

## Architecture

The project follows a layered Data Lake architecture.

```text
NYC TLC Trip Record Data
        |
        v
Landing Zone
data/landing/yellow_taxi/year=2023/month=01..05
        |
        v
PySpark Transformation
- Read original Parquet files
- Standardize schema
- Cast data types
- Apply basic quality filters
- Create analytical columns
        |
        v
Consumption Zone
data/consumption/yellow_taxi_trips
        |
        v
Analysis Layer
- PySpark queries
- SQL reference queries
- Streamlit dashboard
```

## Project Structure

```text
ifood-case/
├─ app/
│  └─ dashboard.py
│
├─ analysis/
│  ├─ answers_pyspark.py
│  ├─ answer_01.sql
│  └─ answer_02.sql
│
├─ src/
│  ├─ config.py
│  ├─ ingest.py
│  ├─ transform.py
│  └─ quality_checks.py
│
├─ outputs/
│  ├─ charts/
│  ├─ tables/
│  └─ quality/
│
├─ data/
│  ├─ landing/
│  └─ consumption/
│
├─ notebooks/
│  └─ exploratory_analysis.ipynb
│
├─ docs/
├─ README.md
├─ requirements.txt
└─ .gitignore
```

## Technologies Used

* Python 3.11
* PySpark
* Apache Spark
* Parquet
* Pandas
* Streamlit
* Plotly
* Git/GitHub

## Data Lake Layers

### Landing Zone

The Landing Zone stores the original files downloaded from the NYC TLC source.

This layer preserves the raw data and allows reprocessing when needed.

Example:

```text
data/landing/yellow_taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet
```

### Consumption Zone

The Consumption Zone stores the transformed and standardized data, ready for analytical usage.

Example:

```text
data/consumption/yellow_taxi_trips
```

The final dataset is partitioned by:

```text
trip_year
trip_month
```

## Final Consumption Schema

| Column           | Type      | Description                          |
| ---------------- | --------- | ------------------------------------ |
| vendor_id        | integer   | Vendor identifier                    |
| passenger_count  | double    | Number of passengers in the trip     |
| total_amount     | double    | Total amount charged for the trip    |
| pickup_datetime  | timestamp | Pickup datetime                      |
| dropoff_datetime | timestamp | Dropoff datetime                     |
| trip_year        | integer   | Year extracted from pickup datetime  |
| trip_month       | integer   | Month extracted from pickup datetime |
| pickup_hour      | integer   | Hour extracted from pickup datetime  |

## Technical Decisions

### PySpark

PySpark was used for the transformation layer because it is suitable for large-scale data processing and is widely adopted in Data Engineering and Data Architecture environments.

### Parquet

Parquet was chosen because it is a columnar storage format optimized for analytical workloads. It provides efficient compression and faster reads for selected columns.

### Layered Architecture

The separation between Landing and Consumption layers improves:

* Traceability
* Reprocessing
* Maintainability
* Governance
* Analytical consumption

### Schema Standardization

During the transformation process, schema differences were identified between monthly Parquet files, especially in the `passenger_count` column.

To handle this, each monthly file is read and standardized individually before being consolidated into the final consumption dataset.

This avoids schema conversion errors and makes the pipeline more robust.

## How to Run

### 1. Create virtual environment

```bash
python -m venv .venv
```

### 2. Activate virtual environment

Windows PowerShell:

```bash
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Java and Hadoop on Windows

For local PySpark execution on Windows, Java and Hadoop winutils are required.

Example environment variables:

```powershell
$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$env:HADOOP_HOME="C:\hadoop"
$env:Path="$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
```

### 5. Run ingestion

```bash
python src/ingest.py
```

### 6. Run transformation

```bash
python src/transform.py
```

### 7. Run analysis

```bash
python analysis/answers_pyspark.py
```

### 8. Run dashboard

```bash
streamlit run app/dashboard.py
```

The dashboard will be available at:

```text
http://localhost:8501
```

## Results

The pipeline successfully processed:

```text
15,616,382 records
```

## Business Questions

### Question 1

What is the average total amount received per month considering all yellow taxis in the fleet?

| Month | Average total_amount |
| ----: | -------------------: |
|     1 |                27.40 |
|     2 |                27.31 |
|     3 |                28.23 |
|     4 |                28.72 |
|     5 |                29.38 |

### Question 2

What is the average number of passengers by pickup hour in May?

| Pickup Hour | Average passenger_count |
| ----------: | ----------------------: |
|           0 |                    1.41 |
|           1 |                    1.42 |
|           2 |                    1.44 |
|           3 |                    1.44 |
|           4 |                    1.39 |
|           5 |                    1.27 |
|           6 |                    1.23 |
|           7 |                    1.25 |
|           8 |                    1.27 |
|           9 |                    1.28 |
|          10 |                    1.32 |
|          11 |                    1.33 |
|          12 |                    1.35 |
|          13 |                    1.36 |
|          14 |                    1.36 |
|          15 |                    1.37 |
|          16 |                    1.37 |
|          17 |                    1.36 |
|          18 |                    1.36 |
|          19 |                    1.37 |
|          20 |                    1.38 |
|          21 |                    1.40 |
|          22 |                    1.41 |
|          23 |                    1.41 |

## SQL Reference

### Average total_amount by month

```sql
SELECT
    trip_month,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM yellow_taxi_trips
WHERE trip_year = 2023
  AND trip_month BETWEEN 1 AND 5
GROUP BY trip_month
ORDER BY trip_month;
```

### Average passenger_count by hour in May

```sql
SELECT
    pickup_hour,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM yellow_taxi_trips
WHERE trip_year = 2023
  AND trip_month = 5
GROUP BY pickup_hour
ORDER BY pickup_hour;
```

## Dashboard

The project includes a Streamlit dashboard that provides:

* Executive overview
* Pipeline process view
* Consumption data sample
* Logical schema
* Analytical results
* Data quality checks
* SQL reference queries

Run with:

```bash
streamlit run app/dashboard.py
```

## Data Quality

The transformation layer applies basic quality rules:

* Remove records with null `pickup_datetime`
* Remove records with null `dropoff_datetime`
* Remove records with null `passenger_count`
* Remove records with null `total_amount`
* Remove records with negative `passenger_count`
* Remove records with negative `total_amount`
* Keep only trips from January to May 2023

## Production Evolution

In a production environment, this architecture could evolve to:

* Amazon S3 as the Data Lake storage layer
* AWS Glue Catalog or Unity Catalog for metadata management
* Athena, Databricks SQL or Trino for query consumption
* Delta Lake for ACID transactions and schema evolution
* Apache Airflow or Databricks Workflows for orchestration
* Great Expectations for data quality validation
* CloudWatch or Datadog for monitoring and observability
* CI/CD pipelines for automated deployment

## Author

Luciano Junior Machado Gonçalves

Software Engineer focused on Data Engineering, Cloud Architecture, Backend Engineering, DevSecOps and AI applied to business operations.
