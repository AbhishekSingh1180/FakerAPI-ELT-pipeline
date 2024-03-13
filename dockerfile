# Use the official Airflow image as the base image
FROM apache/airflow:latest

# Install additional provider packages
RUN pip install apache-airflow-providers-snowflake apache-airflow-providers-google snowflake-connector-python pandas requests