#!/bin/bash
# Ponto único de configuração da instalação (versões e bibliotecas).
# Lido (source) pelos scripts de instalação.

AIRFLOW_VERSION=3.0.2
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Bibliotecas Python usadas pelas DAGs (além das que já vêm com o Airflow).
# Para incluir ou remover uma lib, basta editar esta lista.
BIBLIOTECAS=(
  pandas
)
