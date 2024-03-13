# Leveraged Airflow for ELT Pipeline Orchestration in Snowflake with FakerAPI

![Diagram](https://github.com/AbhishekSingh1180/FakerAPI-ELT-pipeline/blob/main/diagram/etl_pipeline.png)

## Overview

This project demonstrates the orchestration of an Extract, Load, and Transform (ELT) pipeline using Apache Airflow within a Snowflake environment. The pipeline extracts product information from a Faker API, stages it in Google Cloud Storage (GCS), and performs transformations using stored procedures in Snowflake.

## Key Components

- Apache Airflow:
    Airflow is utilized as the orchestration tool for managing and scheduling the pipeline tasks.
    Docker with a local executor simplifies setup and deployment.
- Data Extraction:
    Data is extracted from a Faker API, providing synthetic product information.
- Staging Area:
    Google Cloud Storage (GCS) is used for staging extracted data before loading it into Snowflake.
- Transformation:
    Data transformation is performed within Snowflake using stored procedures.

## Workflow

- Data Extraction:
    Airflow triggers a task to extract data from the Faker API.
    Extracted data is stored in GCS for further processing.
- Data Loading:
    Airflow tasks load the data from GCS into Snowflake tables.
- Transformation:
    Stored procedures within Snowflake are executed to transform the data based on predefined business logic.
- Orchestration and Monitoring:
    Airflow manages the workflow, scheduling tasks, and monitoring execution.
    DAG visualizations provide insights into the pipeline's progress and dependencies.