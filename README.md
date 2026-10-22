# Airflow + Postgres com Docker

Ambiente de laboratório com dois containers:

| Serviço    | Container              | Imagem                     | Porta |
|------------|------------------------|----------------------------|-------|
| `postgres` | `laboratorio-postgres` | `postgres`                 | 5432  |
| `airflow`  | `laboratorio-airflow`  | `laboratorio/airflow` (build local via `Dockerfile`) | 8080  |

## Estrutura

```
.
├── Dockerfile                        # imagem do airflow (ubuntu:24.04 + dependências)
├── docker-compose.yml
├── dags/                             # montado em /home/airflow/airflow/dags
├── initdb/                           # scripts SQL executados na criação do Postgres
└── scripts/
    ├── preparar_ambiente.sh      # executado no build da imagem
    └── instalar_airflow.sh           # instala o Airflow em um venv
```

## Como funciona o build

O serviço `airflow` é construído a partir do `Dockerfile`:

1. Parte de `ubuntu:24.04`.
2. Copia `scripts/preparar_ambiente.sh` para `/tmp`.
3. Executa o script (`apt-get` com `python3-pip`, `python3-venv`, `python3-dev`, `libpq-dev`, `libssl-dev`, `nano`, `sudo`) e o remove.

As dependências ficam em uma camada em cache, então não são reinstaladas a cada recriação do container.

## Uso

Construir e subir:

```
docker compose up -d --build
```

Entrar no container do Airflow:

```
docker exec -it laboratorio-airflow bash
```

Parar:

```
docker compose down
```

Mudanças em `scripts/preparar_ambiente.sh` só valem depois de um novo `--build`.

## Observações

- Os scripts `.sh` devem usar fim de linha **LF**. Com CRLF (comum no Windows), o build falha com `\r: command not found`. Dica: `*.sh text eol=lf` no `.gitattributes`.
- Nomes de container não aceitam `/`; por isso `laboratorio-postgres` e `laboratorio-airflow`.
- `scripts/instalar_airflow.sh` ainda não é executado automaticamente. Para rodá-lo manualmente: `docker exec laboratorio-airflow bash /caminho/instalar_airflow.sh` (é preciso antes copiá-lo ou montá-lo no container).

## Pendências conhecidas

- O `instalar_airflow.sh` apenas instala o Airflow; não o inicia nem configura a conexão com o Postgres.
- O `command` do serviço `airflow` é `tail -f /dev/null`, só para manter o container vivo.
