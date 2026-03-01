"""Geração de relatórios — Markdown, JSON, HTML."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from horus.analysis import GraphAnalysis
from horus.config import Config
from horus.risk_engine import ResultadoRisco
from horus.utils import get_logger

logger = get_logger(__name__)


class ReportGenerator:
    """Gera relatórios em múltiplos formatos."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()

    def generate(
        self,
        resultado: ResultadoRisco,
        graph_analysis: GraphAnalysis | None = None,
        formato: str = "markdown",
        output_dir: Path | None = None,
    ) -> str:
        """Gera relatório e retorna o caminho do arquivo."""
        output_dir = output_dir or self.config.paths.exports
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"horus_{resultado.cpf_cnpj}_{timestamp}"

        if formato == "json":
            return self._to_json(resultado, graph_analysis, output_dir / f"{base_name}.json")
        elif formato == "html":
            return self._to_html(resultado, graph_analysis, output_dir / f"{base_name}.html")
        else:
            return self._to_markdown(resultado, graph_analysis, output_dir / f"{base_name}.md")

    # ------------------------------------------------------------------
    # Markdown
    # ------------------------------------------------------------------

    def _to_markdown(
        self, resultado: ResultadoRisco, analysis: GraphAnalysis | None, path: Path
    ) -> str:
        lines = [
            f"# 🔍 HORUS — Relatório de Análise",
            "",
            f"> {self.config.disclaimer}",
            "",
            "---",
            "",
            resultado.to_markdown(),
            "",
        ]

        if analysis:
            report = analysis.full_report()
            lines.extend([
                "---",
                "",
                "## Análise do Grafo",
                "",
                f"- **Nós:** {report['metricas'].get('nodes', 0)}",
                f"- **Arestas:** {report['metricas'].get('edges', 0)}",
                f"- **Componentes:** {report['metricas'].get('components', 0)}",
                f"- **Densidade:** {report['metricas'].get('density', 0):.4f}",
                "",
                "### Top Centralidade (Grau)",
                "",
                "| Nó | Tipo | Centralidade |",
                "|----|------|-------------|",
            ])
            for item in report.get("top_degree", []):
                lines.append(
                    f"| {item['label'][:40]} | {item['tipo']} | {item['degree_centrality']:.4f} |"
                )
            lines.extend([
                "",
                "### Top PageRank",
                "",
                "| Nó | Tipo | PageRank |",
                "|----|------|----------|",
            ])
            for item in report.get("top_pagerank", []):
                lines.append(
                    f"| {item['label'][:40]} | {item['tipo']} | {item['pagerank']:.6f} |"
                )
            lines.extend([
                "",
                f"### Comunidades: {len(report.get('comunidades', []))}",
                f"### Hubs: {len(report.get('hubs', []))}",
                f"### Triângulos: {report.get('triangulos', 0)}",
            ])

        lines.extend([
            "",
            "---",
            "",
            f"*Relatório gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} "
            f"pelo HORUS v1.0*",
        ])

        content = "\n".join(lines)
        path.write_text(content, encoding="utf-8")
        logger.info("Relatório Markdown salvo: %s", path)
        return str(path)

    # ------------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------------

    def _to_json(
        self, resultado: ResultadoRisco, analysis: GraphAnalysis | None, path: Path
    ) -> str:
        data: dict[str, Any] = {
            "disclaimer": self.config.disclaimer,
            "resultado": resultado.to_dict(),
        }
        if analysis:
            data["analise_grafo"] = analysis.full_report()

        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Relatório JSON salvo: %s", path)
        return str(path)

    # ------------------------------------------------------------------
    # HTML
    # ------------------------------------------------------------------

    def _to_html(
        self, resultado: ResultadoRisco, analysis: GraphAnalysis | None, path: Path
    ) -> str:
        nivel_color = {
            "Baixo": "#4CAF50",
            "Médio": "#FFC107",
            "Alto": "#FF9800",
            "Muito Alto": "#F44336",
        }
        color = nivel_color.get(resultado.nivel, "#90A4AE")

        indicadores_html = ""
        for ind in sorted(resultado.indicadores, key=lambda x: -x.contribuicao):
            bar_width = max(2, ind.score * 100)
            indicadores_html += f"""
            <div class="indicator">
                <div class="ind-header">
                    <span class="ind-name">{ind.nome}</span>
                    <span class="ind-weight">Peso: {ind.peso:.0f}</span>
                </div>
                <div class="bar-bg">
                    <div class="bar-fill" style="width: {bar_width}%"></div>
                </div>
                <div class="ind-detail">{ind.detalhes}</div>
            </div>
            """

        graph_html = ""
        if analysis:
            report = analysis.full_report()
            graph_html = f"""
            <div class="section">
                <h2>Análise do Grafo</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-value">{report['metricas'].get('nodes', 0)}</div>
                        <div class="stat-label">Nós</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{report['metricas'].get('edges', 0)}</div>
                        <div class="stat-label">Arestas</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{report['metricas'].get('components', 0)}</div>
                        <div class="stat-label">Componentes</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{len(report.get('hubs', []))}</div>
                        <div class="stat-label">Hubs</div>
                    </div>
                </div>
            </div>
            """

        html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HORUS — {resultado.nome}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: #1a1a1a; color: #e0e0e0;
            line-height: 1.6; padding: 2rem;
        }}
        .container {{ max-width: 900px; margin: 0 auto; }}
        .header {{
            text-align: center; padding: 2rem;
            border-bottom: 2px solid #333;
            margin-bottom: 2rem;
        }}
        .header h1 {{ color: #4FC3F7; font-size: 1.8rem; }}
        .disclaimer {{
            background: #2a2a2a; padding: 1rem; border-radius: 8px;
            border-left: 4px solid #FF9800; margin: 1.5rem 0;
            font-size: 0.85rem; color: #bbb;
        }}
        .score-box {{
            text-align: center; padding: 2rem;
            background: #2a2a2a; border-radius: 12px; margin: 1.5rem 0;
        }}
        .score-value {{
            font-size: 4rem; font-weight: bold; color: {color};
        }}
        .score-label {{ font-size: 1.2rem; color: #999; }}
        .nivel {{ font-size: 1.5rem; color: {color}; font-weight: bold; }}
        .section {{ margin: 2rem 0; }}
        .section h2 {{ color: #4FC3F7; margin-bottom: 1rem; border-bottom: 1px solid #333; padding-bottom: 0.5rem; }}
        .indicator {{
            background: #2a2a2a; border-radius: 8px; padding: 1rem;
            margin: 0.8rem 0;
        }}
        .ind-header {{ display: flex; justify-content: space-between; margin-bottom: 0.5rem; }}
        .ind-name {{ font-weight: bold; color: #e0e0e0; }}
        .ind-weight {{ color: #888; font-size: 0.85rem; }}
        .bar-bg {{
            background: #333; border-radius: 4px; height: 8px;
            overflow: hidden; margin: 0.3rem 0;
        }}
        .bar-fill {{
            background: linear-gradient(90deg, #4CAF50, #FF9800, #F44336);
            height: 100%; border-radius: 4px; transition: width 0.5s;
        }}
        .ind-detail {{ font-size: 0.85rem; color: #999; }}
        .stats-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
        }}
        .stat-card {{
            background: #2a2a2a; border-radius: 8px; padding: 1.5rem;
            text-align: center;
        }}
        .stat-value {{ font-size: 2rem; font-weight: bold; color: #4FC3F7; }}
        .stat-label {{ color: #888; font-size: 0.85rem; }}
        .footer {{
            text-align: center; padding: 2rem; color: #555;
            border-top: 1px solid #333; margin-top: 2rem; font-size: 0.8rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 HORUS</h1>
            <p style="color: #888;">Sistema de Análise de Risco em Dados Públicos</p>
        </div>

        <div class="disclaimer">{self.config.disclaimer}</div>

        <div class="score-box">
            <div class="score-label">Score de Risco</div>
            <div class="score-value">{resultado.score_total:.1f}</div>
            <div class="nivel">{resultado.nivel}</div>
            <p style="color: #888; margin-top: 0.5rem;">{resultado.nome} — {resultado.cpf_cnpj}</p>
        </div>

        <div class="section">
            <h2>Indicadores de Risco</h2>
            {indicadores_html}
        </div>

        {graph_html}

        <div class="section">
            <h2>Resumo</h2>
            <p>{resultado.resumo}</p>
        </div>

        <div class="footer">
            Relatório gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
            pelo HORUS v1.0
        </div>
    </div>
</body>
</html>"""

        path.write_text(html, encoding="utf-8")
        logger.info("Relatório HTML salvo: %s", path)
        return str(path)
