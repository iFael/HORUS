"""
Testes de integração REAL — verificam se cada classe ETL consegue
extrair e transformar dados das APIs públicas.

Rodar separadamente:
    python -m pytest tests/test_etl_integration.py -v -s --timeout=120

Nota: Requer internet. Alguns testes podem ser lentos ou falhar por
instabilidade do servidor público (Receita Federal, DATASUS, etc.)
"""
from __future__ import annotations

import os
import pytest
import pandas as pd

os.environ.setdefault("TRANSPARENCIA_API_TOKEN", "")
os.environ.setdefault("LOG_LEVEL", "WARNING")

# Mark para testes de integração (podem ser skipados com -m "not integration")
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def integration_db(tmp_path_factory):
    """Banco compartilhado para todos os testes de integração."""
    tmp = tmp_path_factory.mktemp("integration_data")
    os.environ["DATA_DIR"] = str(tmp)
    from horus.config import Config, Paths
    paths = Paths(
        data=tmp,
        raw=tmp / "raw",
        processed=tmp / "processed",
        exports=tmp / "exports",
        db=tmp / "test.db",
    )
    config = Config(paths=paths)
    from horus.database import DatabaseManager
    return DatabaseManager(config)


# ---------------------------------------------------------------
# 1. BCB (Banco Central) — API pública, sem token
# ---------------------------------------------------------------
class TestBCBIntegration:
    def test_extract_selic(self, integration_db):
        from horus.etl.bcb import BCBETL
        etl = BCBETL(integration_db)
        raw = etl.extract(series=["selic"], data_inicio="01/01/2025", data_fim="31/01/2025")
        assert "selic" in raw
        assert len(raw["selic"]) > 0

    def test_transform(self, integration_db):
        from horus.etl.bcb import BCBETL
        etl = BCBETL(integration_db)
        raw = etl.extract(series=["selic"], data_inicio="01/01/2025", data_fim="31/01/2025")
        df = etl.transform(raw)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "serie" in df.columns


# ---------------------------------------------------------------
# 2. IBGE SIDRA — API pública, sem token
# ---------------------------------------------------------------
class TestIBGEIntegration:
    def test_extract_ipca(self, integration_db):
        from horus.etl.ibge import IBGEETL
        etl = IBGEETL(integration_db)
        raw = etl.extract(tabelas=["ipca_mensal"])
        assert "ipca_mensal" in raw
        assert len(raw["ipca_mensal"]) > 0

    def test_localidades(self, integration_db):
        from horus.etl.ibge import IBGEETL
        etl = IBGEETL(integration_db)
        estados = etl._get_localidades()
        assert len(estados) >= 27  # 26 estados + DF


# ---------------------------------------------------------------
# 3. SICONFI (Tesouro Nacional) — API pública, sem token
# ---------------------------------------------------------------
class TestSICONFIIntegration:
    def test_extract_entes(self, integration_db):
        from horus.etl.siconfi import SICONFIETL
        etl = SICONFIETL(integration_db)
        raw = etl.extract()
        assert "entes" in raw
        assert len(raw["entes"]) > 100

    def test_transform(self, integration_db):
        from horus.etl.siconfi import SICONFIETL
        etl = SICONFIETL(integration_db)
        raw = etl.extract()
        df = etl.transform(raw)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


# ---------------------------------------------------------------
# 4. IPEAData — API pública, sem token
# ---------------------------------------------------------------
class TestIPEADataIntegration:
    def test_extract_pib(self, integration_db):
        from horus.etl.ipeadata import IPEADataETL
        etl = IPEADataETL(integration_db)
        raw = etl.extract(series=["pib_per_capita"])
        assert "pib_per_capita" in raw
        assert len(raw["pib_per_capita"]) > 0

    def test_transform(self, integration_db):
        from horus.etl.ipeadata import IPEADataETL
        etl = IPEADataETL(integration_db)
        raw = etl.extract(series=["pib_per_capita"])
        df = etl.transform(raw)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


# ---------------------------------------------------------------
# 5. Querido Diário — API pública, sem token
# ---------------------------------------------------------------
class TestQueridoDiarioIntegration:
    def test_extract(self, integration_db):
        from horus.etl.diarios import DiariosETL
        etl = DiariosETL(integration_db)
        raw = etl.extract(query="licitacao", max_pages=1)
        assert isinstance(raw, list)
        assert len(raw) > 0

    def test_transform(self, integration_db):
        from horus.etl.diarios import DiariosETL
        etl = DiariosETL(integration_db)
        raw = etl.extract(query="licitacao", max_pages=1)
        df = etl.transform(raw)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "territorio_nome" in df.columns


# ---------------------------------------------------------------
# 6. PNCP — API pública, sem token
# ---------------------------------------------------------------
class TestPNCPIntegration:
    def test_extract_contratacoes(self, integration_db):
        from horus.etl.pncp import PNCPETL
        etl = PNCPETL(integration_db)
        raw = etl.extract(data_inicio="20250101", data_fim="20250131",
                          modalidades=[8], max_pages=1)
        assert isinstance(raw, list)
        assert len(raw) > 0

    def test_transform(self, integration_db):
        from horus.etl.pncp import PNCPETL
        etl = PNCPETL(integration_db)
        raw = etl.extract(data_inicio="20250101", data_fim="20250131",
                          modalidades=[8], max_pages=1)
        df = etl.transform(raw)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "numero" in df.columns


# ---------------------------------------------------------------
# 7. TSE — API CKAN pública
# ---------------------------------------------------------------
class TestTSEIntegration:
    def test_ckan_api_available(self, integration_db):
        """Verifica que o CKAN do TSE responde."""
        import requests
        resp = requests.get("https://dadosabertos.tse.jus.br/api/3/action/package_list",
                            params={"limit": 3}, timeout=30)
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("success") is True


# ---------------------------------------------------------------
# 8. CVM — Downloads CSV
# ---------------------------------------------------------------
class TestCVMIntegration:
    def test_extract_cia_aberta(self, integration_db):
        from horus.etl.cvm import CVMETL
        etl = CVMETL(integration_db)
        raw = etl.extract()
        assert "cia_aberta" in raw

    def test_transform_cia_aberta(self, integration_db):
        from horus.etl.cvm import CVMETL
        etl = CVMETL(integration_db)
        raw = etl.extract()
        transformed = etl.transform(raw)
        assert isinstance(transformed, dict)
        if "empresas_cvm" in transformed:
            assert not transformed["empresas_cvm"].empty


# ---------------------------------------------------------------
# 9. Portal da Transparência — Requer token (skip se sem token)
# ---------------------------------------------------------------
class TestTransparenciaIntegration:
    @pytest.fixture(autouse=True)
    def check_token(self, integration_db):
        token = integration_db.config.transparencia_token
        # conftest define token fake "test_token_123" para unit tests
        if not token or token.startswith("test_"):
            pytest.skip("TRANSPARENCIA_API_TOKEN real não configurado")

    def test_extract_servidores(self, integration_db):
        """Usa endpoint 'servidores' com cpf (não por-nome, que é 403)."""
        from horus.etl.transparencia import TransparenciaETL
        etl = TransparenciaETL(integration_db)
        # CPF fictício — retorna lista vazia, mas sem erro de API
        servidores = etl.extract_servidores(cpf="00000000191")
        assert isinstance(servidores, list)

    def test_extract_contratos(self, integration_db):
        """Contratos requer codigoOrgao (SIAPE)."""
        from horus.etl.transparencia import TransparenciaETL
        etl = TransparenciaETL(integration_db)
        contratos = etl.extract_contratos(codigo_orgao="26246")
        assert isinstance(contratos, list)
        assert len(contratos) > 0  # CGU (26246) tem contratos

    def test_extract_emendas(self, integration_db):
        """Emendas requer ano."""
        from horus.etl.transparencia import TransparenciaETL
        etl = TransparenciaETL(integration_db)
        emendas = etl.extract_emendas(ano=2024)
        assert isinstance(emendas, list)
        assert len(emendas) > 0

    def test_extract_sancoes(self, integration_db):
        """Sanções CGU (CEIS, CNEP, CEAF, CEPIM) — mesma API do Portal."""
        from horus.etl.cgu_sancoes import SancoesETL
        etl = SancoesETL(integration_db)
        raw = etl.extract()
        assert isinstance(raw, dict)
        assert any(k in raw for k in ["CEIS", "CNEP", "CEAF", "CEPIM"])


# ---------------------------------------------------------------
# 10. Receita Federal — servidor instável (skip por padrão)
# ---------------------------------------------------------------
class TestReceitaFederalIntegration:
    def test_list_remote_files(self, integration_db):
        """Tenta listar ZIPs — skip se servidor indisponível."""
        from horus.etl.receita_cnpj import ReceitaCNPJETL
        etl = ReceitaCNPJETL(integration_db)
        try:
            urls = etl._list_remote_files("Empresas")
        except Exception:
            pytest.skip("Servidor dadosabertos.rfb.gov.br indisponível (timeout)")
            return
        if not urls:
            pytest.skip("Servidor dadosabertos.rfb.gov.br sem dados")
        assert len(urls) > 0


# ---------------------------------------------------------------
# 11. DATASUS — apenas verifica portal
# ---------------------------------------------------------------
class TestDATASUSIntegration:
    def test_portal_accessible(self, integration_db):
        import requests
        resp = requests.get("https://dadosabertos.saude.gov.br/dataset/cnes-dados-abertos",
                            timeout=30)
        assert resp.status_code == 200
