"""Testes para raiox.database."""

import pandas as pd
import pytest


class TestDatabaseManager:
    def test_init_creates_tables(self, db):
        stats = db.estatisticas()
        assert "pessoas" in stats
        assert "empresas" in stats
        assert "contratos" in stats

    def test_upsert_df_pessoas(self, db):
        df = pd.DataFrame([
            {"cpf": "12345678900", "nome": "Test User",
             "nome_normalizado": "TEST USER", "atualizado_em": "2024-01-01"},
        ])
        count = db.upsert_df("pessoas", df)
        assert count == 1

        pessoa = db.buscar_pessoa_cpf("12345678900")
        assert pessoa is not None
        assert pessoa["nome"] == "Test User"

    def test_upsert_df_empty(self, db):
        count = db.upsert_df("pessoas", pd.DataFrame())
        assert count == 0

    def test_upsert_df_replace(self, db):
        df1 = pd.DataFrame([
            {"cpf": "11111111111", "nome": "Original",
             "nome_normalizado": "ORIGINAL", "atualizado_em": "2024-01-01"},
        ])
        db.upsert_df("pessoas", df1)

        df2 = pd.DataFrame([
            {"cpf": "11111111111", "nome": "Updated",
             "nome_normalizado": "UPDATED", "atualizado_em": "2024-01-02"},
        ])
        db.upsert_df("pessoas", df2)

        pessoa = db.buscar_pessoa_cpf("11111111111")
        assert pessoa["nome"] == "Updated"

    def test_buscar_pessoa_nome(self, populated_db):
        results = populated_db.buscar_pessoa_nome("SILVA")
        assert len(results) >= 2

    def test_buscar_sancoes(self, populated_db):
        sancoes = populated_db.buscar_sancoes("11111111000100")
        assert len(sancoes) == 1
        assert sancoes[0]["tipo"] == "CEIS"

    def test_buscar_contratos(self, populated_db):
        contratos = populated_db.buscar_contratos_fornecedor("11111111000100")
        assert len(contratos) == 2

    def test_buscar_socios(self, populated_db):
        socios = populated_db.buscar_socios_empresa("11111111000100")
        assert len(socios) == 2

    def test_buscar_empresas_socio(self, populated_db):
        empresas = populated_db.buscar_empresas_socio("12345678900")
        assert len(empresas) >= 1

    def test_buscar_candidaturas(self, populated_db):
        cands = populated_db.buscar_candidaturas("12345678900")
        assert len(cands) == 2

    def test_buscar_servidores(self, populated_db):
        servs = populated_db.buscar_servidores_cpf("12345678900")
        assert len(servs) == 1

    def test_buscar_emendas(self, populated_db):
        emendas = populated_db.buscar_emendas_autor("12345678900")
        assert len(emendas) == 2

    def test_buscar_doacoes(self, populated_db):
        doacoes = populated_db.buscar_doacoes_candidato("12345678900")
        assert len(doacoes) == 1

    def test_cache(self, db):
        assert db.cache_valido("test_key") is False
        db.atualizar_cache("test_key", "test_fonte", 100)
        assert db.cache_valido("test_key") is True

    def test_query_df(self, populated_db):
        df = populated_db.query_df("SELECT * FROM pessoas")
        assert len(df) >= 3
        assert "cpf" in df.columns

    def test_estatisticas(self, populated_db):
        stats = populated_db.estatisticas()
        assert stats["pessoas"] >= 3
        assert stats["empresas"] >= 2
        assert stats["contratos"] >= 2
