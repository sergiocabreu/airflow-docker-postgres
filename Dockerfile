FROM ubuntu:24.04

ENV AIRFLOW_HOME=/home/airflow/airflow

COPY scripts/configuracao_instalacao.sh scripts/preparar_ambiente.sh scripts/instalar_airflow.sh scripts/instalar_bibliotecas.sh /tmp/scripts/

# Cada RUN só executa se o anterior terminar com sucesso
RUN bash /tmp/scripts/preparar_ambiente.sh
RUN bash /tmp/scripts/instalar_airflow.sh
RUN bash /tmp/scripts/instalar_bibliotecas.sh

RUN rm -rf /tmp/scripts

# O start_airflow.sh ativa o venv e roda 'airflow standalone' (processo longo),
# por isso fica no CMD e não em um RUN, que travaria o build.
COPY scripts/start_airflow.sh /usr/local/bin/start_airflow.sh
RUN sed -i 's/\r$//' /usr/local/bin/start_airflow.sh && chmod +x /usr/local/bin/start_airflow.sh

CMD ["/usr/local/bin/start_airflow.sh"]
