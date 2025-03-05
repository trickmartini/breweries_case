FROM apache/airflow:latest

USER root
COPY requirements.txt .

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Switch back to the airflow user for further installations
USER airflow

RUN pip install --no-cache-dir -r requirements.txt
# Install the required Python packages
RUN pip install apache-airflow-providers-apache-spark pyspark
