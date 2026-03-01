"""ETL do IBGE — SIDRA e serviço de dados."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class IBGEETL(BaseETL):
    """Extrator de dados do IBGE (SIDRA + servicodados)."""

    nome_fonte = "ibge"

    # Tabelas SIDRA úteis
    TABELAS_SIDRA = {
        "ipca_mensal": "/t/1737/n1/all/v/2266/p/last%2012/d/v2266%2013",
        "pib_trimestral": "/t/1846/n1/all/v/all/p/last%204",
        "populacao_estimada": "/t/6579/n6/all/v/9324/p/last%201",
    }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get_sidra(self, path: str) -> list[dict]:
        rate_limiter.wait("ibge", max_per_minute=30)
        url = f"{self.config.urls.ibge_sidra}values{path}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get_localidades(self, uf: str = "", municipio: str = "") -> list[dict]:
        """Busca localidades do IBGE (API v1 — estável)."""
        rate_limiter.wait("ibge", max_per_minute=30)
        base = self.config.urls.ibge_servicos
        if municipio:
            url = f"{base}localidades/municipios/{municipio}"
        elif uf:
            url = f"{base}localidades/estados/{uf}/municipios"
        else:
            url = f"{base}localidades/estados"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else [data]

    def extract(self, **kwargs: Any) -> dict[str, Any]:
        tabelas = kwargs.get("tabelas", list(self.TABELAS_SIDRA.keys()))
        result: dict[str, Any] = {}

        for nome in tabelas:
            path = self.TABELAS_SIDRA.get(nome)
            if not path:
                continue
            try:
                data = self._get_sidra(path)
                result[nome] = data
            except Exception as e:
                self.logger.warning("Erro IBGE SIDRA %s: %s", nome, e)

        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for nome, data in raw.items():
            if not data:
                continue
            df = pd.DataFrame(data)
            df["tabela"] = nome
            frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "ibge_dados.csv"
        if dest.exists():
            existing = pd.read_csv(dest)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
