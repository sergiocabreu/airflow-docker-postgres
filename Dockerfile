FROM ubuntu:24.04

ENV AIRFLOW_HOME=/home/airflow/airflow

COPY scripts/preparar_ambiente.sh scripts/instalar_airflow.sh /tmp/scripts/

# Cada RUN só executa se o anterior terminar com sucesso
RUN bash /tmp/scripts/preparar_ambiente.sh
RUN bash /tmp/scripts/instalar_airflow.sh

RUN rm -rf /tmp/scripts
