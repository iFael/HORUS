"""ETL do SIAFI — Sistema Integrado de Administração Financeira (Tesouro Nacional)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class SIAFIETL(BaseETL):
    """Extrator de dados do SIAFI via API do Tesouro Nacional."""

    nome_fonte = "siafi"

    BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siafi/tt"

    ENDPOINTS = {
        "programas_governo": "/programas",
        "execucao_orcamentaria": "/execucao",
    }

    def _get(self, path: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("siafi", max_per_minute=30)
        url = f"{self.BASE_URL}{path}"
        resp = self._session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", data) if isinstance(data, dict) else data

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        ano = kwargs.get("ano", "2025")
        result: dict[str, list[dict]] = {}
        for nome, path in self.ENDPOINTS.items():
            try:
                data = self._get(path, {"an_exercicio": ano})
                result[nome] = data if isinstance(data, list) else [data]
            except Exception as e:
                self.logger.warning("Erro SIAFI %s: %s", nome, e)
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
        dest = self.config.paths.processed / "siafi_dados.csv"
        if dest.exists():
            existing = pd.read_csv(dest, dtype=str)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
