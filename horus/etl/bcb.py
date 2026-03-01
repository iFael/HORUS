"""ETL do Banco Central do Brasil — SGS e PTAX."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class BCBETL(BaseETL):
    """Extrator de séries do BCB (SGS e OData/PTAX)."""

    nome_fonte = "bcb"

    SERIES_UTEIS = {
        "selic": 432,
        "ipca": 433,
        "igpm": 189,
        "cambio_compra": 1,
        "cambio_venda": 10813,
    }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get_sgs(self, serie: int, data_inicio: str = "", data_fim: str = "") -> list[dict]:
        rate_limiter.wait("bcb", max_per_minute=60)
        url = self.config.urls.bcb_sgs.format(serie=serie)
        params: dict[str, str] = {"formato": "json"}
        if data_inicio:
            params["dataInicial"] = data_inicio
        if data_fim:
            params["dataFinal"] = data_fim
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        series = kwargs.get("series", list(self.SERIES_UTEIS.keys()))
        data_inicio = kwargs.get("data_inicio", "01/01/2020")
        data_fim = kwargs.get("data_fim", "")

        result: dict[str, list[dict]] = {}
        for nome in series:
            codigo = self.SERIES_UTEIS.get(nome, nome)
            if isinstance(codigo, str) and codigo.isdigit():
                codigo = int(codigo)
            try:
                data = self._get_sgs(codigo, data_inicio, data_fim)
                result[str(nome)] = data
            except Exception as e:
                self.logger.warning("Erro BCB série %s: %s", nome, e)

        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, items in raw.items():
            if not items:
                continue
            df = pd.DataFrame(items)
            df["serie"] = nome
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "bcb_series.csv"
        if dest.exists():
            existing = pd.read_csv(dest)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
