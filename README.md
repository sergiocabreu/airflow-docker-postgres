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
    ├── configuracao_instalacao.sh    # versões, constraints e lista de bibliotecas
    ├── preparar_ambiente.sh          # executado no build da imagem
    ├── instalar_airflow.sh           # instala o Airflow em um venv
    ├── instalar_bibliotecas.sh       # instala as libs extras das DAGs
    └── start_airflow.sh              # inicia o Airflow (CMD do container)
```

## Como funciona o build

O serviço `airflow` é construído a partir do `Dockerfile`:

1. Parte de `ubuntu:24.04`.
2. Copia os scripts de instalação para `/tmp/scripts`.
3. Executa `preparar_ambiente.sh` (`apt-get` com `python3-pip`, `python3-venv`, `python3-dev`, `libpq-dev`, `libssl-dev`, `nano`, `sudo`).
4. Executa `instalar_airflow.sh` (usuário `airflow`, venv em `/home/airflow/venv`, Airflow com constraints).
5. Executa `instalar_bibliotecas.sh` (libs extras, ex.: `pandas`) e remove `/tmp/scripts`.
6. Define como `CMD` o `start_airflow.sh`, que ativa o venv e roda `airflow standalone`.

Versões e bibliotecas ficam em `scripts/configuracao_instalacao.sh`.

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

Acompanhar os logs do Airflow:

```
docker logs -f laboratorio-airflow
```

Parar:

```
docker compose down
```

Mudanças nos scripts só valem depois de um novo `--build`.

## Acessar o Airflow

Interface: <http://localhost:8080>

O usuário é `admin` e a senha é gerada na primeira inicialização. Para recuperá-la:

```
docker exec laboratorio-airflow cat /home/airflow/airflow/simple_auth_manager_passwords.json.generated
```

A saída tem o formato `{"admin": "<senha>"}`. Ela também aparece nos logs (`docker logs laboratorio-airflow | grep -i password`). O arquivo fica dentro do container, então recriar o container gera uma nova senha.

## Conectar ao Postgres (DBeaver ou outro cliente)

| Campo    | Valor       |
|----------|-------------|
| Host     | `localhost` |
| Porta    | `5432`      |
| Database | `postgres`  |
| Usuário  | `postgres`  |
| Senha    | `postgres`  |

O `initdb/01-init.sql` também cria o banco `airflow` e o usuário `airflow` (senha `1234`).

No DBeaver: **Nova conexão → PostgreSQL**, preencha os campos acima e clique em **Testar conexão** (o driver é baixado na primeira vez).

Pela linha de comando:

```
docker exec -it laboratorio-postgres psql -U postgres
```

Entre containers da rede `laboratorio`, use o host `postgres` (nome do serviço) em vez de `localhost`.

## Observações

- Os scripts `.sh` devem usar fim de linha **LF**. Com CRLF (comum no Windows), o build falha com `\r: command not found`. Dica: `*.sh text eol=lf` no `.gitattributes`.
- Nomes de container não aceitam `/`; por isso `laboratorio-postgres` e `laboratorio-airflow`.
- O `airflow standalone` usa SQLite (`airflow.db` dentro do container); ainda não está configurado para usar o Postgres.
- Se as DAGs não aparecerem, verifique erros de import: `docker exec -u airflow -e AIRFLOW_HOME=/home/airflow/airflow laboratorio-airflow /home/airflow/venv/bin/airflow dags list-import-errors`.
