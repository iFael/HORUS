"""ETL do SIOP — Sistema Integrado de Planejamento e Orçamento."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class SIOPETL(BaseETL):
    """Extrator de dados orçamentários do SIOP."""

    nome_fonte = "siop"

    BASE_URL = "https://www.gov.br/conecta/catalogo/apis/wsquantitativo-do-siop"
    API_URL = "https://api.siop.planejamento.gov.br/services/v1"

    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("siop", max_per_minute=20)
        url = f"{self.API_URL}/{endpoint}"
        resp = self._session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("items", [data])

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        ano = kwargs.get("ano", "2025")
        result: dict[str, list[dict]] = {}
        endpoints = {
            "quantitativo": "quantitativo",
            "qualitativo": "qualitativo",
        }
        for nome, ep in endpoints.items():
            try:
                data = self._get(ep, {"exercicio": ano})
                result[nome] = data
            except Exception as e:
                self.logger.warning("Erro SIOP %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, items in raw.items():
            if not items:
                continue
            df = pd.DataFrame(items)
            df["tipo"] = nome
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "siop_dados.csv"
        df.to_csv(dest, index=False)
        return len(df)
