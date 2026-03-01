"""ETL do SICONFI — Finanças Públicas (Tesouro Nacional)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class SICONFIETL(BaseETL):
    """Extrator de dados do SICONFI (Tesouro Nacional)."""

    nome_fonte = "siconfi"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("siconfi", max_per_minute=55)
        url = f"{self.config.urls.siconfi}{endpoint}"
        resp = requests.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", data) if isinstance(data, dict) else data

    def extract_rreo(self, ente: str, ano: int, periodo: int = 6) -> list[dict]:
        """Relatório Resumido de Execução Orçamentária."""
        return self._get("rreo", {"an_exercicio": ano, "nr_periodo": periodo, "id_ente": ente})

    def extract_rgf(self, ente: str, ano: int, periodo: int = 3) -> list[dict]:
        """Relatório de Gestão Fiscal."""
        return self._get("rgf", {"an_exercicio": ano, "nr_periodo": periodo, "id_ente": ente})

    def extract_entes(self) -> list[dict]:
        """Lista de entes federativos."""
        return self._get("entes")

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        ente = kwargs.get("ente", "")
        ano = kwargs.get("ano", 2023)
        result: dict[str, list[dict]] = {}

        try:
            result["entes"] = self.extract_entes()
        except Exception as e:
            self.logger.warning("Erro SICONFI entes: %s", e)

        if ente:
            try:
                result["rreo"] = self.extract_rreo(ente, ano)
            except Exception as e:
                self.logger.warning("Erro SICONFI RREO: %s", e)
            try:
                result["rgf"] = self.extract_rgf(ente, ano)
            except Exception as e:
                self.logger.warning("Erro SICONFI RGF: %s", e)

        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        frames = []
        for tipo, items in raw.items():
            if not items:
                continue
            df = pd.DataFrame(items)
            df["tipo_relatorio"] = tipo
            frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "siconfi_dados.csv"
        if dest.exists():
            existing = pd.read_csv(dest)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        df.to_csv(dest, index=False)
        return len(df)
