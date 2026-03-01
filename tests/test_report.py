"""Testes para raiox.report."""

import pytest
from pathlib import Path

from raiox.report import ReportGenerator
from raiox.risk_engine import RiskEngine
from raiox.analysis import GraphAnalysis
from raiox.graph_builder import GraphBuilder


class TestReportGenerator:
    def _get_resultado(self, populated_db):
        engine = RiskEngine(populated_db)
        return engine.calcular_risco_cpf("12345678900")

    def _get_analysis(self, populated_db):
        gb = GraphBuilder(populated_db)
        gb.build_from_cpf("12345678900", profundidade=1)
        return GraphAnalysis(gb)

    def test_generate_markdown(self, populated_db, tmp_path):
        resultado = self._get_resultado(populated_db)
        analysis = self._get_analysis(populated_db)
        gen = ReportGenerator(populated_db.config)
        path = gen.generate(resultado, analysis, formato="markdown", output_dir=tmp_path)
        assert Path(path).exists()
        content = Path(path).read_text(encoding="utf-8")
        assert "RaioX Público BR" in content
        assert "JOAO DA SILVA" in content

    def test_generate_json(self, populated_db, tmp_path):
        resultado = self._get_resultado(populated_db)
        gen = ReportGenerator(populated_db.config)
        path = gen.generate(resultado, formato="json", output_dir=tmp_path)
        assert Path(path).exists()
        import json
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        assert "resultado" in data
        assert "disclaimer" in data

    def test_generate_html(self, populated_db, tmp_path):
        resultado = self._get_resultado(populated_db)
        analysis = self._get_analysis(populated_db)
        gen = ReportGenerator(populated_db.config)
        path = gen.generate(resultado, analysis, formato="html", output_dir=tmp_path)
        assert Path(path).exists()
        content = Path(path).read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content
        assert "JOAO DA SILVA" in content
        assert "#1a1a1a" in content  # dark theme

    def test_generate_without_analysis(self, populated_db, tmp_path):
        resultado = self._get_resultado(populated_db)
        gen = ReportGenerator(populated_db.config)
        path = gen.generate(resultado, formato="markdown", output_dir=tmp_path)
        assert Path(path).exists()
