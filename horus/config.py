"""Configuração centralizada do HORUS."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Streamlit Cloud: secrets são injetados via st.secrets → variáveis de ambiente
try:
    import streamlit as _st
    for _k, _v in _st.secrets.items():
        if isinstance(_v, str):
            os.environ.setdefault(_k, _v)
except Exception:
    pass  # Fora do Streamlit ou sem secrets configurados

# ---------------------------------------------------------------------------
# Diretórios
# ---------------------------------------------------------------------------
_BASE = Path(__file__).resolve().parent.parent
_DATA = Path(os.getenv("DATA_DIR", _BASE / "data"))


@dataclass(frozen=True)
class Paths:
    base: Path = _BASE
    data: Path = _DATA
    raw: Path = field(default_factory=lambda: _DATA / "raw")
    processed: Path = field(default_factory=lambda: _DATA / "processed")
    exports: Path = field(default_factory=lambda: Path(os.getenv("EXPORTS_DIR", _DATA / "exports")))
    db: Path = field(default_factory=lambda: _DATA / "horus.db")

    def ensure(self) -> None:
        for p in (self.raw, self.processed, self.exports):
            p.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# URLs das APIs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class URLs:
    # Portal da Transparência (CGU)
    transparencia: str = "https://api.portaldatransparencia.gov.br/api-de-dados"
    # Sanções CGU
    ceis: str = "https://api.portaldatransparencia.gov.br/api-de-dados/ceis"
    cnep: str = "https://api.portaldatransparencia.gov.br/api-de-dados/cnep"
    ceaf: str = "https://api.portaldatransparencia.gov.br/api-de-dados/ceaf"
    cepim: str = "https://api.portaldatransparencia.gov.br/api-de-dados/cepim"
    # Servidores
    servidores: str = "https://api.portaldatransparencia.gov.br/api-de-dados/servidores"
    # Contratos
    contratos: str = "https://api.portaldatransparencia.gov.br/api-de-dados/contratos"
    # Licitações
    licitacoes: str = "https://api.portaldatransparencia.gov.br/api-de-dados/licitacoes"
    # Emendas
    emendas: str = "https://api.portaldatransparencia.gov.br/api-de-dados/emendas"
    # PNCP
    pncp: str = "https://pncp.gov.br/api/consulta/v1"
    # Receita Federal CNPJ
    receita_cnpj: str = "https://dadosabertos.rfb.gov.br/CNPJ/"
    # TSE
    tse: str = "https://dadosabertos.tse.jus.br/dataset/"
    # BCB
    bcb_sgs: str = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
    bcb_ptax: str = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    # CVM
    cvm: str = "https://dados.cvm.gov.br/dados/"
    # IBGE
    ibge_sidra: str = "https://apisidra.ibge.gov.br/"
    ibge_servicos: str = "https://servicodados.ibge.gov.br/api/v1/"
    # SICONFI
    siconfi: str = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/"
    # IPEAData
    ipeadata: str = "http://www.ipeadata.gov.br/api/odata4/"
    # Querido Diário
    querido_diario: str = "https://api.queridodiario.ok.org.br/api/gazettes"
    # DATASUS
    datasus: str = "https://datasus.saude.gov.br/transferencia-de-arquivos/"
    # ANVISA
    anvisa: str = "https://dados.anvisa.gov.br/dados/"
    # ANEEL
    aneel: str = "https://dadosabertos.aneel.gov.br/api/3/action/"
    # Câmara dos Deputados
    camara: str = "https://dadosabertos.camara.leg.br/api/v2"
    # Senado Federal
    senado: str = "https://legis.senado.leg.br/dadosabertos"


# ---------------------------------------------------------------------------
# Parâmetros de Risk Engine
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RiskParams:
    # Pesos dos indicadores (nome → peso)
    pesos: dict[str, float] = field(default_factory=lambda: {
        "sancao_ativa": 20.0,
        "empresa_familiar_contrato": 15.0,
        "concentracao_contratos": 12.0,
        "doador_contratado": 12.0,
        "empresa_recem_criada": 10.0,
        "mesmo_endereco_multiplos_cnpj": 10.0,
        "emenda_autodirecionada": 8.0,
        "variacao_patrimonial": 8.0,
        "inexigibilidade_alta": 7.0,
        "acumulacao_cargos": 5.0,
        "parente_fornecedor": 8.0,
        "contrato_vespera_eleicao": 6.0,
        "sobrepreco_estimado": 9.0,
        "aditivo_excessivo": 7.0,
        "empresa_socio_pep": 10.0,
    })
    # Limiar (fração) de concentração para alerta
    concentracao_limiar: float = 0.40
    # Idade mínima da empresa (anos) para ser considerada recém-criada
    empresa_idade_min: float = 2.0
    # Variação patrimonial anormal (em fração; 3.0 = 300%)
    variacao_patrimonial_limiar: float = 3.0
    # Valor mínimo de inexigibilidade para alerta (R$)
    inexigibilidade_valor_min: float = 500_000.0


# ---------------------------------------------------------------------------
# Config principal
# ---------------------------------------------------------------------------
@dataclass
class Config:
    """Configuração global do sistema."""

    paths: Paths = field(default_factory=Paths)
    urls: URLs = field(default_factory=URLs)
    risk: RiskParams = field(default_factory=RiskParams)

    # Token do Portal da Transparência
    transparencia_token: str = field(
        default_factory=lambda: os.getenv("TRANSPARENCIA_API_TOKEN", "")
    )

    # Geral
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    cache_ttl_days: int = field(
        default_factory=lambda: int(os.getenv("CACHE_TTL_DAYS", "30"))
    )
    etl_workers: int = field(
        default_factory=lambda: int(os.getenv("ETL_WORKERS", "4"))
    )
    streamlit_port: int = field(
        default_factory=lambda: int(os.getenv("STREAMLIT_PORT", "8501"))
    )

    # Disclaimer padrão
    disclaimer: str = (
        "Esta é uma análise baseada exclusivamente em dados públicos abertos. "
        "Ela indica APENAS padrões estatísticos de risco e NÃO constitui prova "
        "de irregularidade ou crime. Qualquer uso deve ser validado por "
        "profissionais e órgãos competentes."
    )

    def __post_init__(self) -> None:
        self.paths.ensure()
