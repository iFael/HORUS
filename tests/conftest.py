"""Fixtures compartilhadas para testes."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Forçar variável de ambiente antes de importar anything
# Preservar token real se definido via env/dotenv; usar fake apenas para unit tests
os.environ.setdefault("TRANSPARENCIA_API_TOKEN", "test_token_123")
os.environ["LOG_LEVEL"] = "WARNING"


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Diretório temporário para dados."""
    data = tmp_path / "data"
    data.mkdir()
    (data / "raw").mkdir()
    (data / "processed").mkdir()
    (data / "exports").mkdir()
    return data


@pytest.fixture
def config(tmp_data_dir: Path):
    """Config com diretório temporário."""
    os.environ["DATA_DIR"] = str(tmp_data_dir)
    from horus.config import Config, Paths
    paths = Paths(
        data=tmp_data_dir,
        raw=tmp_data_dir / "raw",
        processed=tmp_data_dir / "processed",
        exports=tmp_data_dir / "exports",
        db=tmp_data_dir / "test.db",
    )
    c = Config(paths=paths)
    c.transparencia_token = "test_token_123"
    return c


@pytest.fixture
def db(config):
    """Instância de DatabaseManager com banco temporário."""
    from horus.database import DatabaseManager
    return DatabaseManager(config)


@pytest.fixture
def populated_db(db):
    """Banco com dados de exemplo para testes."""
    import pandas as pd

    # Pessoas
    db.upsert_df("pessoas", pd.DataFrame([
        {"cpf": "12345678900", "nome": "JOAO DA SILVA",
         "nome_normalizado": "JOAO DA SILVA", "uf": "SP", "municipio": "SAO PAULO",
         "atualizado_em": "2024-01-01"},
        {"cpf": "98765432100", "nome": "MARIA DA SILVA",
         "nome_normalizado": "MARIA DA SILVA", "uf": "SP", "municipio": "SAO PAULO",
         "atualizado_em": "2024-01-01"},
        {"cpf": "11122233344", "nome": "PEDRO SANTOS",
         "nome_normalizado": "PEDRO SANTOS", "uf": "RJ", "municipio": "RIO DE JANEIRO",
         "atualizado_em": "2024-01-01"},
    ]))

    # Empresas
    db.upsert_df("empresas", pd.DataFrame([
        {"cnpj": "11111111000100", "razao_social": "EMPRESA SILVA LTDA",
         "data_abertura": "2023-01-01", "situacao": "ATIVA", "uf": "SP",
         "municipio": "SAO PAULO", "endereco": "RUA A 100 CENTRO",
         "capital_social": 100000, "atualizado_em": "2024-01-01"},
        {"cnpj": "22222222000100", "razao_social": "CONSTRUTORA XYZ SA",
         "data_abertura": "2010-06-15", "situacao": "ATIVA", "uf": "SP",
         "municipio": "SAO PAULO", "endereco": "RUA A 100 CENTRO",
         "capital_social": 5000000, "atualizado_em": "2024-01-01"},
    ]))

    # Sócios
    db.upsert_df("socios", pd.DataFrame([
        {"cnpj": "11111111000100", "tipo_socio": "PF",
         "cpf_cnpj_socio": "12345678900", "nome_socio": "JOAO DA SILVA",
         "qualificacao": "Sócio-Administrador", "data_entrada": "2023-01-01",
         "atualizado_em": "2024-01-01"},
        {"cnpj": "11111111000100", "tipo_socio": "PF",
         "cpf_cnpj_socio": "98765432100", "nome_socio": "MARIA DA SILVA",
         "qualificacao": "Sócio", "data_entrada": "2023-01-01",
         "atualizado_em": "2024-01-01"},
    ]))

    # Contratos
    db.upsert_df("contratos", pd.DataFrame([
        {"numero": "CT-001", "orgao": "MINISTERIO DA SAUDE",
         "orgao_cnpj": "00394544000185", "fornecedor_cnpj": "11111111000100",
         "fornecedor_nome": "EMPRESA SILVA LTDA", "objeto": "Fornecimento de equipamentos",
         "valor": 500000, "data_inicio": "2024-01-15", "data_fim": "2024-12-31",
         "modalidade": "Pregao eletronico", "fonte": "transparencia",
         "atualizado_em": "2024-01-01"},
        {"numero": "CT-002", "orgao": "MINISTERIO DA SAUDE",
         "orgao_cnpj": "00394544000185", "fornecedor_cnpj": "11111111000100",
         "fornecedor_nome": "EMPRESA SILVA LTDA", "objeto": "Serviços de consultoria",
         "valor": 300000, "data_inicio": "2024-03-01", "data_fim": "2024-09-30",
         "modalidade": "inexigibilidade", "fonte": "transparencia",
         "atualizado_em": "2024-01-01"},
    ]))

    # Sanções
    db.upsert_df("sancoes", pd.DataFrame([
        {"tipo": "CEIS", "cpf_cnpj": "11111111000100",
         "nome": "EMPRESA SILVA LTDA",
         "orgao_sancionador": "CGU", "fundamentacao": "Lei 12846",
         "data_inicio": "2024-01-01", "data_fim": "", "uf": "SP",
         "fonte": "transparencia", "atualizado_em": "2024-01-01"},
    ]))

    # Candidaturas
    db.upsert_df("candidaturas", pd.DataFrame([
        {"cpf": "12345678900", "nome": "JOAO DA SILVA", "ano_eleicao": 2018,
         "cargo": "VEREADOR", "partido": "PXX", "uf": "SP", "municipio": "SAO PAULO",
         "situacao": "ELEITO", "total_bens": 100000, "total_receitas": 50000,
         "total_despesas": 45000, "votos": 5000, "atualizado_em": "2024-01-01"},
        {"cpf": "12345678900", "nome": "JOAO DA SILVA", "ano_eleicao": 2022,
         "cargo": "DEPUTADO ESTADUAL", "partido": "PXX", "uf": "SP",
         "municipio": "SAO PAULO", "situacao": "ELEITO", "total_bens": 1500000,
         "total_receitas": 200000, "total_despesas": 180000, "votos": 50000,
         "atualizado_em": "2024-01-01"},
    ]))

    # Servidores
    db.upsert_df("servidores", pd.DataFrame([
        {"cpf": "12345678900", "nome": "JOAO DA SILVA",
         "orgao": "CAMARA MUNICIPAL SP", "orgao_cnpj": "46395000000139",
         "cargo": "VEREADOR", "funcao": "", "remuneracao": 18000,
         "data_ingresso": "2019-01-01", "situacao_vinculo": "ATIVO",
         "uf": "SP", "atualizado_em": "2024-01-01"},
    ]))

    # Emendas
    db.upsert_df("emendas", pd.DataFrame([
        {"numero": "EM-001", "autor": "JOAO DA SILVA", "autor_cpf": "12345678900",
         "tipo": "Individual", "ano": 2023, "valor_empenhado": 1000000,
         "valor_pago": 800000, "localidade": "SAO PAULO", "uf": "SP",
         "funcao": "Saude", "subfuncao": "", "atualizado_em": "2024-01-01"},
        {"numero": "EM-002", "autor": "JOAO DA SILVA", "autor_cpf": "12345678900",
         "tipo": "Individual", "ano": 2023, "valor_empenhado": 500000,
         "valor_pago": 300000, "localidade": "CAMPINAS", "uf": "SP",
         "funcao": "Educacao", "subfuncao": "", "atualizado_em": "2024-01-01"},
    ]))

    # Doações
    db.upsert_df("doacoes", pd.DataFrame([
        {"cpf_cnpj_doador": "11111111000100", "nome_doador": "EMPRESA SILVA LTDA",
         "cpf_candidato": "12345678900", "nome_candidato": "JOAO DA SILVA",
         "ano_eleicao": 2022, "valor": 50000, "tipo_recurso": "Fundo Eleitoral",
         "partido": "PXX", "atualizado_em": "2024-01-01"},
    ]))

    return db
