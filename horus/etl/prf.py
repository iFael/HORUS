"""ETL da PRF — Polícia Rodoviária Federal (dados abertos de acidentes)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class PRFETL(BaseETL):
    """Extrator de dados de acidentes da PRF."""

    nome_fonte = "prf"

    BASE_URL = "https://dados.gov.br/dados/api/3/action"

    DATASETS = {
        "acidentes": "acidentes-de-transito-nas-rodovias-federais",
    }

    def _get_dataset(self, dataset_id: str) -> dict:
        rate_limiter.wait("prf", max_per_minute=15)
        url = f"{self.BASE_URL}/package_show"
        resp = self._session.get(url, params={"id": dataset_id}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("result", {})

    def extract(self, **kwargs: Any) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for nome, ds_id in self.DATASETS.items():
            try:
                result[nome] = self._get_dataset(ds_id)
            except Exception as e:
                self.logger.warning("Erro PRF %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        records = []
        for nome, ds in raw.items():
            for r in ds.get("resources", []):
                records.append({
                    "dataset": nome,
                    "recurso": r.get("name", ""),
                    "formato": r.get("format", ""),
                    "url": r.get("url", ""),
                    "atualizado_em": r.get("last_modified", ""),
                })
        return pd.DataFrame(records) if records else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "prf_acidentes.csv"
        df.to_csv(dest, index=False)
        return len(df)
