"""ETL do INEP — Censo Escolar e microdados educacionais."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class INEPETL(BaseETL):
    """Extrator de dados educacionais do INEP."""

    nome_fonte = "inep"

    # API Dados Abertos INEP
    BASE_URL = "https://dadosabertos.inep.gov.br/api/3/action"

    DATASETS = {
        "censo_escolar": "censo-escolar",
        "enem": "microdados-do-enem",
        "ideb": "ideb",
    }

    def _search(self, dataset_id: str) -> dict:
        rate_limiter.wait("inep", max_per_minute=20)
        url = f"{self.BASE_URL}/package_show"
        resp = self._session.get(url, params={"id": dataset_id}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("result", {})

    def extract(self, **kwargs: Any) -> dict[str, dict]:
        datasets = kwargs.get("datasets", list(self.DATASETS.keys()))
        result: dict[str, dict] = {}
        for nome in datasets:
            ds_id = self.DATASETS.get(nome, nome)
            try:
                data = self._search(ds_id)
                result[nome] = data
            except Exception as e:
                self.logger.warning("Erro INEP %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        records = []
        for nome, ds in raw.items():
            resources = ds.get("resources", [])
            for r in resources:
                records.append({
                    "dataset": nome,
                    "recurso": r.get("name", ""),
                    "formato": r.get("format", ""),
                    "url": r.get("url", ""),
                    "tamanho": r.get("size", 0),
                    "atualizado_em": r.get("last_modified", ""),
                })
        return pd.DataFrame(records) if records else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "inep_dados.csv"
        df.to_csv(dest, index=False)
        return len(df)
