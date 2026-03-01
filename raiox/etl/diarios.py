"""ETL do Querido Diário — Diários Oficiais municipais."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from raiox.etl.base import BaseETL
from raiox.utils import rate_limiter


class DiariosETL(BaseETL):
    """Extrator do Querido Diário (OKBR)."""

    nome_fonte = "diarios"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, params: dict | None = None) -> dict:
        rate_limiter.wait("querido_diario", max_per_minute=30)
        resp = requests.get(self.config.urls.querido_diario, params=params or {}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def extract(self, **kwargs: Any) -> list[dict]:
        """Busca diários por palavra-chave (nome de pessoa, empresa, etc.)."""
        query = kwargs.get("query", "")
        territorio = kwargs.get("territorio", "")
        max_pages = kwargs.get("max_pages", 3)

        if not query:
            return []

        all_data: list[dict] = []
        for offset in range(0, max_pages * 10, 10):
            params: dict[str, Any] = {
                "querystring": query,
                "offset": offset,
                "size": 10,
            }
            if territorio:
                params["territory_id"] = territorio

            try:
                data = self._get(params)
                gazettes = data.get("gazettes", [])
                if not gazettes:
                    break
                all_data.extend(gazettes)
            except Exception as e:
                self.logger.warning("Erro Querido Diário: %s", e)
                break

        return all_data

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame()

        rows = []
        for item in raw:
            rows.append({
                "data": item.get("date", ""),
                "territorio_id": item.get("territory_id", ""),
                "territorio_nome": item.get("territory_name", ""),
                "uf": item.get("state_code", ""),
                "url": item.get("url", ""),
                "excertos": " | ".join(item.get("excerpts", [])[:3]),
                "fonte": "querido_diario",
            })

        return pd.DataFrame(rows)

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        # Diários são usados para análise, não persistidos em tabela própria
        # Armazenamos como cache processado
        if df.empty:
            return 0

        dest = self.config.paths.processed / "diarios_mencoes.csv"
        if dest.exists():
            existing = pd.read_csv(dest)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
                subset=["data", "territorio_id", "url"]
            )
        df.to_csv(dest, index=False)
        self.logger.info("Salvos %d registros de diários em %s", len(df), dest)
        return len(df)
