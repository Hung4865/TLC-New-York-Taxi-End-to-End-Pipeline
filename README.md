

A complete data engineering project for building a modern batch and streaming data pipeline using the NYC Taxi dataset.  
The project demonstrates how raw taxi trip data can be ingested, processed, validated, stored in a data lake/lakehouse, and transformed into an analytics-ready data warehouse.

## 1. Project Overview

This project simulates a real-world data platform with both **batch processing** and **stream processing**.

The main goals are:

- Ingest NYC Taxi trip data from Parquet files.
- Store raw and processed data in a data lake.
- Process large datasets using Apache Spark.
- Simulate real-time data streaming using PostgreSQL, Debezium, Kafka, and Spark Structured Streaming.
- Store reliable processed data using Delta Lake.
- Query data lake tables using Trino and Hive Metastore.
- Build an analytics-ready data warehouse using dbt.
- Validate data quality using Great Expectations.
- Orchestrate the whole workflow using Apache Airflow.

## 2. System Architecture

![System Architecture](<Diagram (3).png>)

The architecture contains the following major layers:

| Layer | Technology | Main Responsibility |
|---|---|---|
| Data Source | NYC Taxi Parquet files | Original taxi trip dataset |
| Data Lake Storage | Hadoop | Store raw, processed, and Delta Lake data |
| Batch Processing | Apache Spark | Clean and transform historical data |
| Stream Simulation | PostgreSQL + Debezium + Kafka | Simulate real-time CDC events |
| Stream Processing | Spark Structured Streaming | Process streaming records from Kafka |
| Lakehouse Layer | Delta Lake | Store reliable, versioned, ACID-compliant data |
| Metadata Layer | Hive Metastore | Store table metadata and schema information |
| Query Engine | Trino | Query data directly from the data lake |
| Data Warehouse | PostgreSQL | Store analytics-ready fact and dimension tables |
| Transformation | dbt | Build star schema models |
| Data Quality | Great Expectations | Validate data before/after transformation |
| Orchestration | Apache Airflow | Schedule and monitor pipeline tasks |

## 3. Datawarehouse Schema
![System Architecture](<Schema.png>)


