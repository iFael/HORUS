"""ETL do CAR/SICAR — Cadastro Ambiental Rural."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class CARETL(BaseETL):
    """Extrator de dados do CAR/SICAR."""

    nome_fonte = "car_sicar"

    BASE_URL = "https://car.gov.br/publico/api/v1"

    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("car", max_per_minute=15)
        url = f"{self.BASE_URL}/{endpoint}"
        resp = self._session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("items", [data])

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        uf = kwargs.get("uf", "")
        result: dict[str, list[dict]] = {}
        endpoints = {
            "resumo_estados": "dashboard/estados",
            "imoveis": "imoveis",
        }
        for nome, ep in endpoints.items():
            try:
                params = {"uf": uf} if uf else {}
                data = self._get(ep, params)
                result[nome] = data
            except Exception as e:
                self.logger.warning("Erro CAR %s: %s", nome, e)
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
        dest = self.config.paths.processed / "car_sicar_dados.csv"
        df.to_csv(dest, index=False)
        return len(df)
