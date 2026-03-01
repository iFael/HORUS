"""ETL do INPE — DETER e PRODES (desmatamento via TerraBrasilis)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class INPEETL(BaseETL):
    """Extrator de dados de desmatamento do INPE (TerraBrasilis)."""

    nome_fonte = "inpe"

    BASE_URL = "http://terrabrasilis.dpi.inpe.br/api/v1"

    ENDPOINTS = {
        "deter_amazonia": "/alerts?biome=amazon",
        "deter_cerrado": "/alerts?biome=cerrado",
        "prodes_amazonia": "/prodes?biome=amazon",
    }

    def _get(self, path: str) -> list[dict]:
        rate_limiter.wait("inpe", max_per_minute=15)
        url = f"{self.BASE_URL}{path}"
        resp = self._session.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("features", [data])

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        endpoints = kwargs.get("endpoints", list(self.ENDPOINTS.keys()))
        result: dict[str, list[dict]] = {}
        for nome in endpoints:
            path = self.ENDPOINTS.get(nome)
            if not path:
                continue
            try:
                result[nome] = self._get(path)
            except Exception as e:
                self.logger.warning("Erro INPE %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, items in raw.items():
            if not items:
                continue
            df = pd.DataFrame(items)
            df["fonte"] = nome
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "inpe_desmatamento.csv"
        if dest.exists():
            existing = pd.read_csv(dest, dtype=str)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
