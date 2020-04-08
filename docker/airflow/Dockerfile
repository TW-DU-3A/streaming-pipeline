# Upstream: https://github.com/puckel/docker-airflow/blob/1.10.4/Dockerfile
FROM puckel/docker-airflow

# For SequentialExecutor, apply relevant subset of https://github.com/puckel/docker-airflow/pull/359/files for airflow scheduler to run:
USER root

RUN pip install hdfs
RUN pip install pyarrow
RUN pip install pandas -U
RUN sed -i 's/LocalExecutor/SequentialExecutor/' /entrypoint.sh

USER airflow

ENV EXECUTOR Sequential
HEALTHCHECK CMD "[ -f /usr/local/airflow/airflow-webserver.pid ]"
