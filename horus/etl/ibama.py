"""ETL do IBAMA — Embargos, Licenciamento e SINAFLOR."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class IBAMAETL(BaseETL):
    """Extrator de dados ambientais do IBAMA (dados abertos)."""

    nome_fonte = "ibama"

    BASE_URL = "https://dadosabertos.ibama.gov.br"

    DATASETS = {
        "embargos": "/api/3/action/package_show?id=fiscalizacao-termo-de-embargo",
        "licenciamento": "/api/3/action/package_show?id=licenciamento-ambiental",
        "autos_infracao": "/api/3/action/package_show?id=fiscalizacao-auto-de-infracao",
    }

    def _get_dataset(self, path: str) -> dict:
        rate_limiter.wait("ibama", max_per_minute=20)
        url = f"{self.BASE_URL}{path}"
        resp = self._session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json().get("result", {})

    def _download_csv(self, url: str) -> pd.DataFrame | None:
        """Baixa e parseia CSV de recurso CKAN."""
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
                self.logger.warning("Erro download CSV IBAMA: %s", e)
                return None

    def extract(self, **kwargs: Any) -> dict[str, dict]:
        datasets = kwargs.get("datasets", list(self.DATASETS.keys()))
        result: dict[str, dict] = {}
        for nome in datasets:
            path = self.DATASETS.get(nome)
            if not path:
                continue
            try:
                result[nome] = self._get_dataset(path)
            except Exception as e:
                self.logger.warning("Erro IBAMA %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, ds in raw.items():
            resources = ds.get("resources", [])
            # Pegar o CSV mais recente
            csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
            if csv_resources:
                url = csv_resources[0].get("url", "")
                if url:
                    df = self._download_csv(url)
                    if df is not None and not df.empty:
                        df["fonte_dataset"] = nome
                        frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "ibama_dados.csv"
        if dest.exists():
            existing = pd.read_csv(dest, dtype=str)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
