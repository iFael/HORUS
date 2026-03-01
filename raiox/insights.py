"""Modelo de Insights / Irregularidades detectadas."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any

import pandas as pd

from raiox.database import DatabaseManager
from raiox.utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Severidades
# ---------------------------------------------------------------------------

class Severidade:
    CRITICO = "CRITICO"
    ALTO = "ALTO"
    MEDIO = "MEDIO"
    BAIXO = "BAIXO"

    _ORDER = {"CRITICO": 4, "ALTO": 3, "MEDIO": 2, "BAIXO": 1}

    @classmethod
    def cor(cls, sev: str) -> str:
        return {
            "CRITICO": "#ff2d2d",
            "ALTO": "#ff6b35",
            "MEDIO": "#ffc107",
            "BAIXO": "#4caf50",
        }.get(sev, "#888")


# ---------------------------------------------------------------------------
# Insight dataclass
# ---------------------------------------------------------------------------

@dataclass
class Insight:
    """Representa uma irregularidade ou padrão suspeito detectado."""

    tipo: str
    titulo: str
    descricao: str
    severidade: str
    score: float  # 0-100
    valor_exposicao: float = 0.0
    pattern: str = ""
    fontes: list[str] = field(default_factory=list)
    politico_id: str = ""
    politico_nome: str = ""
    dados: dict[str, Any] = field(default_factory=dict)
    id: str = ""

    def __post_init__(self) -> None:
        if not self.id:
            raw = f"{self.tipo}:{self.politico_id}:{self.titulo}:{self.valor_exposicao}"
            self.id = hashlib.sha256(raw.encode()).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "politico_id": self.politico_id,
            "politico_nome": self.politico_nome,
            "tipo": self.tipo,
            "titulo": self.titulo,
            "descricao": self.descricao,
            "severidade": self.severidade,
            "score": round(self.score, 1),
            "valor_exposicao": self.valor_exposicao,
            "pattern": self.pattern,
            "fontes": ",".join(self.fontes),
            "dados_json": json.dumps(self.dados, ensure_ascii=False, default=str),
            "criado_em": datetime.now().isoformat(),
            "atualizado_em": datetime.now().isoformat(),
        }


# ---------------------------------------------------------------------------
# InsightManager
# ---------------------------------------------------------------------------

class InsightManager:
    """Gerencia persistência e consulta de insights."""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def salvar(self, insight: Insight) -> None:
        """Persiste um insight no banco."""
        df = pd.DataFrame([insight.to_dict()])
        self.db.upsert_df("insights", df)

    def salvar_batch(self, insights: list[Insight]) -> int:
        """Persiste vários insights."""
        if not insights:
            return 0
        rows = [i.to_dict() for i in insights]
        df = pd.DataFrame(rows)
        return self.db.upsert_df("insights", df)

    def limpar_politico(self, politico_id: str) -> None:
        """Remove insights de um político (antes de re-scan)."""
        with self.db.connect() as conn:
            conn.execute("DELETE FROM insights WHERE politico_id = ?", (politico_id,))

    def limpar_todos(self) -> None:
        """Remove todos os insights."""
        with self.db.connect() as conn:
            conn.execute("DELETE FROM insights")

    def get_todos(self, severidade: str = "", limite: int = 500) -> list[dict]:
        return self.db.buscar_insights(severidade=severidade, limite=limite)

    def get_por_politico(self, politico_id: str) -> list[dict]:
        return self.db.buscar_insights(politico_id=politico_id)

    def get_exposicao_total(self) -> float:
        rows = self.db.query("SELECT COALESCE(SUM(valor_exposicao), 0) AS total FROM insights")
        return rows[0]["total"] if rows else 0.0

    def get_contagem_severidade(self) -> dict[str, int]:
        rows = self.db.query(
            "SELECT severidade, COUNT(*) AS cnt FROM insights GROUP BY severidade"
        )
        return {r["severidade"]: r["cnt"] for r in rows}

    def get_top_politicos(self, limite: int = 20) -> list[dict]:
        """Top políticos por exposição total."""
        return self.db.query("""
            SELECT politico_id, politico_nome,
                   COUNT(*) as total_insights,
                   SUM(valor_exposicao) as exposicao_total,
                   MAX(score) as max_score,
                   GROUP_CONCAT(DISTINCT severidade) as severidades
            FROM insights
            GROUP BY politico_id
            ORDER BY exposicao_total DESC
            LIMIT ?
        """, (limite,))


def formatar_valor(valor: float) -> str:
    """Formata valor em Real (R$1.2M, R$800K, R$5.5K, etc.)."""
    if valor >= 1_000_000_000:
        return f"R${valor / 1_000_000_000:.1f}B"
    if valor >= 1_000_000:
        return f"R${valor / 1_000_000:.1f}M"
    if valor >= 1_000:
        return f"R${valor / 1_000:.0f}K"
    return f"R${valor:,.0f}"
