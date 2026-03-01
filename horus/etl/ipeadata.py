"""ETL do IPEAData — Indicadores socioeconômicos via OData."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class IPEADataETL(BaseETL):
    """Extrator de dados do IPEAData (OData 4)."""

    nome_fonte = "ipeadata"

    SERIES_UTEIS = {
        "pib_per_capita": "BM12_PIB12",
        "gini": "BM12_GINI12",
        "idh": "ADH_IDH",
        "taxa_homicidios": "SIM_TXHOM",
        "taxa_pobreza": "PNAD_TXPOB",
    }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, path: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("ipeadata", max_per_minute=30)
        url = f"{self.config.urls.ipeadata}{path}"
        resp = requests.get(url, params=params or {}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("value", data) if isinstance(data, dict) else data

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        series = kwargs.get("series", list(self.SERIES_UTEIS.keys()))
        result: dict[str, list[dict]] = {}

        for nome in series:
            codigo = self.SERIES_UTEIS.get(nome, nome)
            try:
                data = self._get(f"Metadados('{codigo}')/Valores")
                result[nome] = data
            except Exception as e:
                self.logger.warning("Erro IPEAData %s: %s", nome, e)

        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, items in raw.items():
            if not items:
                continue
            df = pd.DataFrame(items)
            df["serie"] = nome
            frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "ipeadata_series.csv"
        if dest.exists():
            existing = pd.read_csv(dest)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
