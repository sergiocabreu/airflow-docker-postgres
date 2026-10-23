#!/bin/bash
set -e

# Bibliotecas e constraints do Airflow definidas em configuracao_instalacao.sh
source "$(dirname "$0")/configuracao_instalacao.sh"

/home/airflow/venv/bin/pip install "${BIBLIOTECAS[@]}" --constraint "${CONSTRAINT_URL}"

echo "Bibliotecas instaladas: ${BIBLIOTECAS[*]}"
