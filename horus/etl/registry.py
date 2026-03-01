"""Registro central de módulos ETL — status, última execução, saúde.

Cada módulo ETL é registrado aqui com seu status real (testado).
O scanner e o dashboard consomem este registro.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable


class ETLStatus(str, Enum):
    ATIVO = "ATIVO"          # API testada e funcionando
    INATIVO = "INATIVO"      # API fora do ar / URL quebrada
    INTEGRADO = "INTEGRADO"  # Já integrado no pipeline (Câmara, Senado, etc.)


@dataclass
class ETLEntry:
    """Registro de um módulo ETL."""
    nome: str                          # Nome interno (ex: "bcb")
    descricao: str                     # Nome legível (ex: "Banco Central - SGS")
    modulo: str                        # Caminho do módulo (ex: "horus.etl.bcb")
    classe: str                        # Nome da classe (ex: "BCBETL")
    status: ETLStatus = ETLStatus.ATIVO
    ultima_execucao: str | None = None
    ultimo_erro: str | None = None
    registros_coletados: int = 0
    url_base: str = ""
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Registro global de todos os módulos ETL
# ---------------------------------------------------------------------------

REGISTRY: list[ETLEntry] = [
    # === JÁ INTEGRADOS NO PIPELINE (scanner.py chama diretamente) ===
    ETLEntry("camara", "Câmara dos Deputados", "horus.etl.camara", "CamaraETL",
             ETLStatus.INTEGRADO, tags=["legislativo", "despesas"]),
    ETLEntry("senado", "Senado Federal", "horus.etl.senado", "SenadoETL",
             ETLStatus.INTEGRADO, tags=["legislativo"]),
    ETLEntry("transparencia", "Portal da Transparência", "horus.etl.transparencia", "TransparenciaETL",
             ETLStatus.INTEGRADO, tags=["contratos", "emendas", "servidores"]),
    ETLEntry("cgu_sancoes", "CGU Sanções (CEIS/CNEP/CEAF/CEPIM)", "horus.etl.cgu_sancoes", "SancoesETL",
             ETLStatus.INTEGRADO, tags=["sancoes"]),
    ETLEntry("pncp", "Portal Nacional de Contratações Públicas", "horus.etl.pncp", "PNCPETL",
             ETLStatus.INTEGRADO, tags=["contratos", "licitacoes"]),
    ETLEntry("tse", "Tribunal Superior Eleitoral", "horus.etl.tse", "TSEETL",
             ETLStatus.INTEGRADO, tags=["eleicoes", "doacoes"]),
    ETLEntry("receita_cnpj", "Receita Federal (CNPJ)", "horus.etl.receita_cnpj", "ReceitaCNPJETL",
             ETLStatus.INTEGRADO, tags=["empresas", "socios"]),

    # === ATIVOS — API testada e funcionando ===
    ETLEntry("bcb", "Banco Central do Brasil (21 séries SGS)", "horus.etl.bcb", "BCBETL",
             ETLStatus.ATIVO, url_base="https://api.bcb.gov.br/dados/serie/",
             tags=["economia", "indicadores"]),
    ETLEntry("ibge", "IBGE SIDRA (11 tabelas)", "horus.etl.ibge", "IBGEETL",
             ETLStatus.ATIVO, url_base="https://apisidra.ibge.gov.br/",
             tags=["economia", "indicadores", "censo"]),
    ETLEntry("cvm", "CVM (6 datasets)", "horus.etl.cvm", "CVMETL",
             ETLStatus.ATIVO, url_base="https://dados.cvm.gov.br/dados/",
             tags=["mercado", "empresas"]),
    ETLEntry("siconfi", "SICONFI - Finanças Públicas", "horus.etl.siconfi", "SICONFIETL",
             ETLStatus.ATIVO, url_base="https://apidatalake.tesouro.gov.br/ords/siconfi/",
             tags=["orcamento", "fiscal"]),
    ETLEntry("ipeadata", "IPEAData", "horus.etl.ipeadata", "IPEADataETL",
             ETLStatus.ATIVO, url_base="http://www.ipeadata.gov.br/api/odata4/",
             tags=["economia", "indicadores"]),
    ETLEntry("diarios", "Querido Diário", "horus.etl.diarios", "DiariosETL",
             ETLStatus.ATIVO, url_base="https://api.queridodiario.ok.org.br/api/",
             tags=["diarios", "transparencia"]),
    ETLEntry("aneel", "ANEEL - Energia Elétrica", "horus.etl.aneel", "ANEELETL",
             ETLStatus.ATIVO, url_base="https://dadosabertos.aneel.gov.br/api/3/action/",
             tags=["regulatorio", "energia"]),
    ETLEntry("antt", "ANTT - Transportes Terrestres", "horus.etl.antt", "ANTTETL",
             ETLStatus.ATIVO, url_base="https://dados.antt.gov.br/api/3/action/",
             tags=["regulatorio", "transportes"]),
    ETLEntry("siafi", "Tesouro Nacional (Execução Orçamentária)", "horus.etl.siafi", "SIAFIETL",
             ETLStatus.ATIVO, url_base="https://apidatalake.tesouro.gov.br/ords/siconfi/",
             tags=["orcamento", "fiscal"]),
    ETLEntry("inpe", "INPE - Desmatamento (DETER/PRODES)", "horus.etl.inpe", "INPEETL",
             ETLStatus.ATIVO, url_base="http://terrabrasilis.dpi.inpe.br/geoserver/",
             tags=["ambiental", "desmatamento"]),
    ETLEntry("datasus", "DataSUS (SIA/SIH/SIM/SINAN)", "horus.etl.datasus", "DataSUSETL",
             ETLStatus.ATIVO, url_base="https://datasus.saude.gov.br/",
             tags=["saude"]),

    # === INATIVOS — API fora do ar ou requer autenticação ===
    ETLEntry("dados_abertos", "Portal Dados Abertos (CKAN)", "horus.etl.dados_abertos", "DadosAbertosETL",
             ETLStatus.INATIVO, ultimo_erro="API CKAN dados.gov.br retorna HTML, não JSON",
             tags=["catalogo"]),
    ETLEntry("tcu", "TCU - Tribunal de Contas", "horus.etl.tcu", "TCUETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar; API TCU requer auth",
             tags=["fiscalizacao"]),
    ETLEntry("dou", "DOU - Diário Oficial da União", "horus.etl.dou", "DOUETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar; INLABS requer cadastro",
             tags=["diarios", "legislacao"]),
    ETLEntry("datajud", "DataJud / CNJ", "horus.etl.datajud", "DataJudETL",
             ETLStatus.INATIVO, ultimo_erro="API requer autenticação (HTTP 401)",
             tags=["judiciario"]),
    ETLEntry("ibama", "IBAMA - Fiscalização Ambiental", "horus.etl.ibama", "IBAMAETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dadosabertos.ibama.gov.br retorna 403",
             tags=["ambiental"]),
    ETLEntry("inep", "INEP - Educação", "horus.etl.inep", "INEPETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dadosabertos.inep.gov.br inacessível",
             tags=["educacao"]),
    ETLEntry("anvisa", "ANVISA - Vigilância Sanitária", "horus.etl.anvisa", "ANVISAETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.anvisa.gov.br inacessível",
             tags=["saude", "regulatorio"]),
    ETLEntry("ans", "ANS - Saúde Suplementar", "horus.etl.ans", "ANSETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dadosabertos.ans.gov.br retorna 404",
             tags=["saude", "regulatorio"]),
    ETLEntry("anatel", "ANATEL - Telecomunicações", "horus.etl.anatel", "ANATALETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN anatel.gov.br retorna HTML",
             tags=["regulatorio"]),
    ETLEntry("dnit", "DNIT - Infraestrutura", "horus.etl.dnit", "DNITETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.dnit.gov.br inacessível",
             tags=["infraestrutura"]),
    ETLEntry("antaq", "ANTAQ - Transportes Aquaviários", "horus.etl.antaq", "ANTAQETL",
             ETLStatus.INATIVO, ultimo_erro="API REST web.antaq.gov.br connection reset",
             tags=["regulatorio", "transportes"]),
    ETLEntry("prf", "PRF - Acidentes Rodoviários", "horus.etl.prf", "PRFETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar",
             tags=["seguranca"]),
    ETLEntry("anac", "ANAC - Aviação Civil", "horus.etl.anac", "ANACETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar",
             tags=["regulatorio", "transportes"]),
    ETLEntry("ancine", "ANCINE - Cinema/Audiovisual", "horus.etl.ancine", "ANCINEETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar",
             tags=["cultura"]),
    ETLEntry("dataprev", "DATAPREV/INSS - Benefícios", "horus.etl.dataprev", "DATAPREVET",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar",
             tags=["previdencia"]),
    ETLEntry("siop", "SIOP - Orçamento Público", "horus.etl.siop", "SIOPETL",
             ETLStatus.INATIVO, ultimo_erro="API api.siop.planejamento.gov.br inacessível",
             tags=["orcamento"]),
    ETLEntry("car_sicar", "CAR/SICAR - Cadastro Ambiental Rural", "horus.etl.car_sicar", "CARSICARETL",
             ETLStatus.INATIVO, ultimo_erro="API car.gov.br inacessível",
             tags=["ambiental"]),
    ETLEntry("anp", "ANP - Petróleo/Gás", "horus.etl.anp", "ANPETL",
             ETLStatus.INATIVO, ultimo_erro="CKAN dados.gov.br fora do ar",
             tags=["regulatorio", "energia"]),
]


def get_registry() -> list[ETLEntry]:
    """Retorna o registro completo."""
    return REGISTRY


def get_active_extras() -> list[ETLEntry]:
    """Retorna módulos ATIVOS que NÃO são INTEGRADOS (fontes complementares)."""
    return [e for e in REGISTRY if e.status == ETLStatus.ATIVO]


def get_all_by_status() -> dict[str, list[ETLEntry]]:
    """Agrupa por status."""
    result: dict[str, list[ETLEntry]] = {s.value: [] for s in ETLStatus}
    for entry in REGISTRY:
        result[entry.status.value].append(entry)
    return result


def get_entry(nome: str) -> ETLEntry | None:
    """Busca entrada por nome."""
    for e in REGISTRY:
        if e.nome == nome:
            return e
    return None


def update_execution(nome: str, registros: int = 0, erro: str | None = None) -> None:
    """Atualiza resultado da última execução."""
    entry = get_entry(nome)
    if entry:
        entry.ultima_execucao = datetime.now().isoformat()
        entry.registros_coletados = registros
        if erro:
            entry.ultimo_erro = erro
        else:
            entry.ultimo_erro = None
