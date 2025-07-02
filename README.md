# NYC Taxi Spark Pipeline 🚖

![Spark](https://img.shields.io/badge/Spark-4.0.0-orange)
![PySpark](https://img.shields.io/badge/PySpark-Pipeline-brightgreen)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue)
![Docker](https://img.shields.io/badge/Containerized-Docker-informational)

This repository contains an end-to-end data engineering pipeline that processes NYC Yellow Taxi trip data using Apache Spark, validates its quality, transforms it into dimensional model format, and prepares it for loading into a PostgreSQL database—all within a Dockerized environment.

---

## 🗂 Project Structure
nyc_taxi_spark_pipeline/
├── data/                          # Raw input CSVs
├── output/                        # Cleaned & transformed parquet data
├── logs/quality/                 # JSON quality reports
├── scripts/                      # Spark job scripts
├── utils/                        # Spark session utils
├── docker/                       # Dockerfiles for Spark/Postgres
├── jars/                         # PostgreSQL JDBC driver
├── requirements.txt              # Python dependencies
└── README.md
---

## 🔁 Pipeline Workflow

1. **Raw Input**: `data/yellow_tripdata_sample.csv`
2. **Cleaning**: Remove nulls, sanitize types via `transform_tripdata.py`
3. **Modeling**: Generate dimension & fact tables via `model_to_fact_dim.py`
4. **Data Quality Check**: Run validation via `data_quality_check.py`
5. **Load**: Write into PostgreSQL via `load_to_postgres.py`

All orchestrated through `run_pipeline.py`.

---

## 📊 Data Quality Report Summary

Generated at: `2025-07-01 23:25:02`

| Metric                          | Value       |
|----------------------------------|-------------|
| Total Rows                      | 5,000       |
| Total Columns                   | 10          |
| Missing Values                  | ✅ None     |
| Out-of-Range Values             | ✅ None     |
| Invalid Payment Types           | ✅ 0        |
| Duplicate Composite Keys        | ✅ 0        |

Full report: [`logs/quality/data_quality_report.json`](logs/quality/data_quality_report.json)

---

## 🛠 Tech Stack

- [Apache Spark 4.0](https://spark.apache.org/)
- [PySpark](https://spark.apache.org/docs/latest/api/python/)
- [PostgreSQL](https://www.postgresql.org/)
- [Docker](https://www.docker.com/)
- [Airflow (Planned)](https://airflow.apache.org/)

---

## ▶️ Run the Pipeline

```bash
# Clean & transform
spark-submit scripts/run_pipeline.py

# Run data quality checks
spark-submit scripts/data_quality_check.py