"""ETL do Portal Dados Abertos — CKAN API (dados.gov.br)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class DadosAbertosETL(BaseETL):
    """Consulta conjuntos de dados via API CKAN do dados.gov.br."""

    nome_fonte = "dados_abertos"

    BASE_URL = "https://dados.gov.br/dados/api/3/action"

    def _search(self, query: str, rows: int = 20) -> list[dict]:
        rate_limiter.wait("dados_abertos", max_per_minute=30)
        url = f"{self.BASE_URL}/package_search"
        resp = self._session.get(url, params={"q": query, "rows": rows}, timeout=30)
        resp.raise_for_status()
        return resp.json().get("result", {}).get("results", [])

    def _show(self, dataset_id: str) -> dict:
        rate_limiter.wait("dados_abertos", max_per_minute=30)
        url = f"{self.BASE_URL}/package_show"
        resp = self._session.get(url, params={"id": dataset_id}, timeout=30)
        resp.raise_for_status()
        return resp.json().get("result", {})

    def extract(self, **kwargs: Any) -> list[dict]:
        query = kwargs.get("query", "transparencia governo federal")
        rows = kwargs.get("rows", 50)
        return self._search(query, rows)

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame()
        records = []
        for ds in raw:
            records.append({
                "id": ds.get("id", ""),
                "titulo": ds.get("title", ""),
                "organizacao": ds.get("organization", {}).get("title", ""),
                "descricao": (ds.get("notes") or "")[:500],
                "num_recursos": len(ds.get("resources", [])),
                "atualizado_em": ds.get("metadata_modified", ""),
            })
        return pd.DataFrame(records)

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "dados_abertos_catalogo.csv"
        df.to_csv(dest, index=False)
        return len(df)
