"""ETL da ANEEL — Agência Nacional de Energia Elétrica (dados abertos)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class ANEELETL(BaseETL):
    """Extrator de dados da ANEEL via API CKAN."""

    nome_fonte = "aneel"

    BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action"

    DATASETS = {
        "geracao_distribuida": "relacao-de-empreendimentos-de-geracao-distribuida",
        "usinas": "siga-sistema-de-informacoes-de-geracao-da-aneel",
        "tarifas": "tarifas-de-energia-eletrica",
        "compensacao_ambiental": "compensacao-financeira-pela-utilizacao-de-recursos-hidricos",
    }

    def _get_dataset(self, dataset_id: str) -> dict:
        rate_limiter.wait("aneel", max_per_minute=20)
        url = f"{self.BASE_URL}/package_show"
        resp = self._session.get(url, params={"id": dataset_id}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("result", {})

    def _download_csv(self, url: str) -> pd.DataFrame | None:
        try:
            resp = self._session.get(url, timeout=120)
            resp.raise_for_status()
            import io
            return pd.read_csv(io.StringIO(resp.text), sep=";", encoding="latin-1",
                               dtype=str, on_bad_lines="skip")
        except Exception:
            try:
                import io
                return pd.read_csv(io.StringIO(resp.text), sep=",", encoding="utf-8",
                                   dtype=str, on_bad_lines="skip")
            except Exception as e:
                self.logger.warning("Erro download CSV ANEEL: %s", e)
                return None

    def extract(self, **kwargs: Any) -> dict[str, dict]:
        datasets = kwargs.get("datasets", list(self.DATASETS.keys()))
        result: dict[str, dict] = {}
        for nome in datasets:
            ds_id = self.DATASETS.get(nome, nome)
            try:
                result[nome] = self._get_dataset(ds_id)
            except Exception as e:
                self.logger.warning("Erro ANEEL %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, ds in raw.items():
            resources = ds.get("resources", [])
            csv_res = [r for r in resources if r.get("format", "").upper() == "CSV"]
            if csv_res:
                url = csv_res[0].get("url", "")
                if url:
                    df = self._download_csv(url)
                    if df is not None and not df.empty:
                        df["fonte_dataset"] = nome
                        frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "aneel_dados.csv"
        if dest.exists():
            existing = pd.read_csv(dest, dtype=str)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
