"""ETL da ANTAQ — Agência Nacional de Transportes Aquaviários."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class ANTAQETL(BaseETL):
    """Extrator de dados da ANTAQ (dados abertos)."""

    nome_fonte = "antaq"

    BASE_URL = "https://web.antaq.gov.br/api/v1"

    ENDPOINTS = {
        "estatisticas_portuarias": "/estatisticas",
        "instalacoes_portuarias": "/instalacoes",
    }

    def _get(self, path: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("antaq", max_per_minute=15)
        url = f"{self.BASE_URL}{path}"
        resp = self._session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("items", [data])

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        for nome, path in self.ENDPOINTS.items():
            try:
                result[nome] = self._get(path)
            except Exception as e:
                self.logger.warning("Erro ANTAQ %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, items in raw.items():
            if not items:
                continue
            df = pd.DataFrame(items)
            df["endpoint"] = nome
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "antaq_dados.csv"
        if dest.exists():
            existing = pd.read_csv(dest, dtype=str)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
