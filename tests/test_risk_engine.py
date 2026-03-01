"""Testes para raiox.risk_engine."""

import pytest

from raiox.risk_engine import RiskEngine, ResultadoRisco, IndicadorRisco


class TestRiskEngine:
    def test_calcular_risco_cpf(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cpf("12345678900")
        assert isinstance(resultado, ResultadoRisco)
        assert 0 <= resultado.score_total <= 100
        assert resultado.nivel in ("Baixo", "Médio", "Alto", "Muito Alto")
        assert len(resultado.indicadores) > 0

    def test_calcular_risco_cnpj(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cnpj("11111111000100")
        assert isinstance(resultado, ResultadoRisco)
        assert 0 <= resultado.score_total <= 100

    def test_indicador_sancao_detectada(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cnpj("11111111000100")
        sancao_ind = [i for i in resultado.indicadores if "Sanção" in i.nome]
        assert len(sancao_ind) > 0
        # CNPJ tem sanção CEIS, deve ter score > 0
        assert sancao_ind[0].score > 0

    def test_indicador_variacao_patrimonial(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cpf("12345678900")
        vp = [i for i in resultado.indicadores if "patrimonial" in i.nome.lower()]
        assert len(vp) > 0
        # Bens: 100k → 1.5M = 1400% de variação
        assert vp[0].score > 0

    def test_indicador_doador_contratado(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cpf("12345678900")
        dc = [i for i in resultado.indicadores if "Doador" in i.nome]
        assert len(dc) > 0
        # CNPJ 11111111000100 doou e tem contratos
        assert dc[0].score > 0

    def test_cpf_sem_dados(self, db):
        engine = RiskEngine(db)
        resultado = engine.calcular_risco_cpf("99999999999")
        assert resultado.score_total == 0
        assert resultado.nivel == "Baixo"

    def test_resultado_to_dict(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cpf("12345678900")
        d = resultado.to_dict()
        assert "score_total" in d
        assert "nivel" in d
        assert "indicadores" in d
        assert isinstance(d["indicadores"], list)

    def test_resultado_to_markdown(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cpf("12345678900")
        md = resultado.to_markdown()
        assert "# Análise de Risco" in md
        assert "JOAO DA SILVA" in md

    def test_nivel_calculado(self):
        r = ResultadoRisco(cpf_cnpj="123", nome="T", score_total=10, nivel="")
        assert r.nivel_calculado == "Baixo"
        r.score_total = 40
        assert r.nivel_calculado == "Médio"
        r.score_total = 60
        assert r.nivel_calculado == "Alto"
        r.score_total = 90
        assert r.nivel_calculado == "Muito Alto"

    def test_indicador_emenda_autodirecionada(self, populated_db):
        engine = RiskEngine(populated_db)
        resultado = engine.calcular_risco_cpf("12345678900")
        em = [i for i in resultado.indicadores if "Emenda" in i.nome]
        assert len(em) > 0
        # 1/2 emendas no domicílio (SAO PAULO)
        # ratio = 0.5, > 0.3, deve pontuar
        assert em[0].score > 0
