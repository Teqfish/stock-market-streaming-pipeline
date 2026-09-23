FROM apache/airflow:3.3.2-python3.12

COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

COPY producer/src /opt/airflow/pipeline/producer

ENV PYTHONPATH="/opt/airflow/pipeline/producer:${PYTHONPATH}"
