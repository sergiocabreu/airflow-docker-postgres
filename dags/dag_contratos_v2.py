"""
DAG de ETL dos contratos do Ceara Transparente -> PostgreSQL (versao 2).

Diferencas em relacao a v1 (api_contratos_ceara_postgresql):
  - Particiona a carga por MES usando dynamic task mapping (.expand).
    Cada fatia (mes) e uma task independente que faz coletar+preparar+salvar
    inteiramente dentro de si -> os dados NAO trafegam por XCom.
  - Periodo configuravel por lista de anos (ANOS). Incluir outro ano = editar a lista.
  - Upsert (ON CONFLICT DO UPDATE): contratos ja existentes sao ATUALIZADOS,
    pois a API atualiza registros antigos retroativamente (ver campo updated_at).
  - Paginacao em lotes: cada pagina e preparada e salva na hora, mantendo o uso
    de memoria baixo mesmo com milhares de registros por mes.
  - Concorrencia limitada (max_active_tis_per_dag) para nao sobrecarregar a API
    publica nem o PostgreSQL.

Observacoes sobre a API (confirmadas em resposta real):
  - A chave de paginacao e "sumary" (sem o segundo "m") -> MANTER assim.
  - O campo de edital vem como "descriaco_edital" (typo da origem) -> MANTER assim.
"""
import calendar
from datetime import timedelta

import pandas as pd
import pendulum
import psycopg2
import psycopg2.extras
import requests
from airflow.sdk import DAG, task

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------
API_URL = "https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos"

# Conexao com o Postgres.
# host = nome do container do Postgres (os dois containers na mesma rede Docker)
# port = porta INTERNA do container (5432), nao a mapeada no host (5433)
PG_HOST = "aula_postgres"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "1234"
PG_DATABASE = "airflow"
PG_TABLE = "contratos"

TIMEZONE = "America/Fortaleza"

# Anos a carregar. Para incluir outro ano, basta adiciona-lo aqui.
#   - periodo fixo e explicito: [2025, 2026]
#   - continuo: list(range(2025, 2027))
#   - "ate o ano atual": list(range(2025, pendulum.now(TIMEZONE).year + 1))
ANOS = [2025, 2026]

# Numero maximo de fatias (meses) processadas em paralelo.
# Protege a API publica e o PostgreSQL de carga simultanea excessiva.
MAX_FATIAS_SIMULTANEAS = 4

# Tamanho do lote de insercao no PostgreSQL.
PAGE_SIZE_INSERT = 500

# Colunas que queremos manter do retorno da API (na ordem de insercao).
# Observacao: "descriaco_edital" tem typo na origem da API; mantido de proposito.
COLUNAS_ESPERADAS = [
    "id",
    "cod_concedente",
    "cod_financiador",
    "cod_gestora",
    "cod_orgao",
    "cod_secretaria",
    "descricao_modalidade",
    "descricao_objeto",
    "descricao_tipo",
    "descricao_url",
    "data_assinatura",
    "data_processamento",
    "data_termino",
    "flg_tipo",
    "isn_parte_destino",
    "isn_sic",
    "num_spu",
    "valor_contrato",
    "isn_modalidade",
    "isn_entidade",
    "tipo_objeto",
    "num_spu_licitacao",
    "descricao_justificativa",
    "valor_can_rstpg",
    "data_publicacao_portal",
    "descricao_url_pltrb",
    "descricao_url_ddisp",
    "descricao_url_inexg",
    "cod_plano_trabalho",
    "num_certidao",
    "descriaco_edital",
    "cpf_cnpj_financiador",
    "num_contrato",
    "valor_original_concedente",
    "valor_original_contrapartida",
    "valor_atualizado_concedente",
    "valor_atualizado_contrapartida",
    "created_at",
    "updated_at",
    "plain_num_contrato",
    "calculated_valor_aditivo",
    "calculated_valor_ajuste",
    "calculated_valor_empenhado",
    "calculated_valor_pago",
    "contract_type",
    "infringement_status",
    "cod_financiador_including_zeroes",
    "accountability_status",
    "plain_cpf_cnpj_financiador",
    "descricao_situacao",
    "data_publicacao_doe",
    "descricao_nome_credor",
    "isn_parte_origem",
    "data_auditoria",
    "data_termino_original",
    "data_inicio",
    "data_rescisao",
    "confidential",
    "gestor_contrato",
    "data_finalizacao_prestacao_contas",
    "sequential",
    "emergency",
    "law",
    "has_non_profit_transfer",
    "nome_fiscal",
    "emenda_parlamentar",
    "ano_emenda_parlamentar",
    "codigo_emenda_parlamentar",
]


# ---------------------------------------------------------------------------
# Funcoes auxiliares (NAO sao tasks; sao chamadas dentro da task de cada mes)
# ---------------------------------------------------------------------------
def converter_data_assinatura(datas: pd.Series) -> pd.Series:
    """Converte a coluna de data de assinatura para datetime no fuso local.

    Tenta varios formatos possiveis; o que nao casar vira NaT (nulo).
    """
    datas_texto = datas.astype(str).str.strip()
    convertida = pd.Series(pd.NaT, index=datas.index, dtype="datetime64[ns, UTC]")
    formatos = [
        "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d", "%d/%m/%Y",
    ]
    for formato in formatos:
        pendentes = convertida.isna()
        if not pendentes.any():
            break
        convertida.loc[pendentes] = pd.to_datetime(
            datas_texto.loc[pendentes], format=formato, errors="coerce", utc=True
        )
    return convertida.dt.tz_convert(TIMEZONE)


def preparar_pagina(registros: list[dict]) -> list[dict]:
    """Seleciona as colunas esperadas e descarta linhas com data_assinatura invalida.

    Recebe os registros de UMA pagina e devolve a lista pronta para insercao.
    """
    if not registros:
        return []

    df = pd.DataFrame(registros)
    colunas = [c for c in COLUNAS_ESPERADAS if c in df.columns]
    df = df[colunas].copy()

    df["data_assinatura_dt"] = converter_data_assinatura(df["data_assinatura"])
    df = df.dropna(subset=["data_assinatura_dt"]).drop(columns=["data_assinatura_dt"])

    return df.to_dict(orient="records")


def montar_sql_upsert(colunas: list[dict]) -> str:
    """Monta o INSERT ... ON CONFLICT (id) DO UPDATE para as colunas informadas.

    Em caso de conflito de id, ATUALIZA as demais colunas com os valores novos
    (a API atualiza contratos antigos retroativamente, ex.: aditivos, rescisao).
    """
    cols_sql = ", ".join(f'"{c}"' for c in colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    # Atualiza todas as colunas, exceto a chave de conflito (id).
    sets = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in colunas if c != "id"
    )
    return (
        f'INSERT INTO {PG_TABLE} ({cols_sql}) VALUES ({placeholders}) '
        f'ON CONFLICT ("id") DO UPDATE SET {sets}'
    )


def salvar_lote(cur, registros: list[dict]) -> int:
    """Insere/atualiza um lote de registros usando o cursor ja aberto.

    Retorna a quantidade de registros enviados.
    """
    if not registros:
        return 0
    colunas = list(registros[0].keys())
    insert_sql = montar_sql_upsert(colunas)
    valores = [[r.get(c) for c in colunas] for r in registros]
    psycopg2.extras.execute_batch(cur, insert_sql, valores, page_size=PAGE_SIZE_INSERT)
    return len(valores)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
@task
def gerar_periodos() -> list[dict]:
    """Gera uma fatia por mes para cada ano em ANOS.

    Cada item e um dict com rotulo e o intervalo de datas (DD/MM/YYYY) que
    sera usado no filtro da API. O ultimo dia do mes e calculado corretamente
    (28/29/30/31) via calendar.monthrange.
    """
    periodos: list[dict] = []
    for ano in ANOS:
        for mes in range(1, 13):
            ultimo_dia = calendar.monthrange(ano, mes)[1]
            periodos.append({
                "rotulo": f"{ano}-{mes:02d}",
                "inicio": f"01/{mes:02d}/{ano}",
                "fim": f"{ultimo_dia:02d}/{mes:02d}/{ano}",
            })
    print(f"Geradas {len(periodos)} fatias (meses) para os anos {ANOS}.")
    return periodos


@task
def etl_mes(periodo: dict) -> dict:
    """ETL completo de UM mes: coletar + preparar + salvar, em lotes.

    - Pagina a API para o intervalo do mes.
    - Prepara e salva cada pagina na hora (memoria baixa, sem XCom de dados).
    - Usa upsert para refletir atualizacoes de contratos ja existentes.
    """
    rotulo = periodo["rotulo"]
    data_inicio = periodo["inicio"]
    data_fim = periodo["fim"]

    # Sessao HTTP com retry/backoff para robustez em paginacoes longas.
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(
            total=3, backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    total_coletados = 0
    total_salvos = 0

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DATABASE,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                pagina = 1
                while True:
                    params = {
                        "page": pagina,
                        "data_assinatura_inicio": data_inicio,
                        "data_assinatura_fim": data_fim,
                    }
                    resposta = session.get(API_URL, params=params, timeout=60)
                    resposta.raise_for_status()
                    payload = resposta.json()

                    dados_pagina = payload.get("data", [])
                    total_coletados += len(dados_pagina)

                    # Prepara e salva ESTA pagina (nao acumula tudo em memoria).
                    preparados = preparar_pagina(dados_pagina)
                    total_salvos += salvar_lote(cur, preparados)

                    # "sumary" (sem o 2o "m") e a chave real da API. Manter.
                    total_paginas = int(
                        payload.get("sumary", {}).get("total_pages", 1)
                    )
                    if pagina >= total_paginas:
                        break
                    pagina += 1
    finally:
        conn.close()
        session.close()

    print(
        f"[{rotulo}] coletados={total_coletados} salvos/atualizados={total_salvos} "
        f"({data_inicio} a {data_fim})."
    )
    return {"rotulo": rotulo, "coletados": total_coletados, "salvos": total_salvos}


# ---------------------------------------------------------------------------
# Definicao da DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_contratos_ceara_postgresql_v2",
    schedule="0 19 * * *",  # diariamente as 19h (timezone America/Fortaleza)
    start_date=pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
    catchup=False,
    default_args=default_args,
    tags=["ceara", "contratos", "postgresql", "v2", "mensal"],
) as dag:

    periodos = gerar_periodos()

    # Dynamic task mapping: uma instancia de etl_mes por fatia (mes),
    # com no maximo MAX_FATIAS_SIMULTANEAS rodando em paralelo.
    etl_mes.override(
        max_active_tis_per_dag=MAX_FATIAS_SIMULTANEAS
    ).expand(periodo=periodos)
