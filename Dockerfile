FROM apache/airflow:latest


USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get -y install libpq-dev gcc && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt