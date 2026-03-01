"""ETL do Tesouro Nacional — Execução Orçamentária via SICONFI.

A API /ords/siafi/ retorna 404. A API funcional é /ords/siconfi/ que retorna
dados reais de RREO, RGF e DCA (testado em 2026-03-01).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class SIAFIETL(BaseETL):
    """Extrator de execução orçamentária via API Tesouro/SICONFI."""

    nome_fonte = "siafi"

    BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("siafi", max_per_minute=30)
        url = f"{self.BASE_URL}/{endpoint}"
        resp = self._session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", []) if isinstance(data, dict) else data

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        ano = kwargs.get("ano", 2024)
        ente = kwargs.get("ente", 1)  # 1 = União
        result: dict[str, list[dict]] = {}

        # RREO — Relatório Resumido de Execução Orçamentária
        try:
            items = self._get("rreo", {
                "an_exercicio": ano, "nr_periodo": 6,
                "co_tipo_demonstrativo": "RREO", "id_ente": ente,
            })
            result["rreo"] = items
            self.logger.info("SIAFI/RREO: %d registros", len(items))
        except Exception as e:
            self.logger.warning("Erro SIAFI RREO: %s", e)

        # DCA — Declarações Contábeis Anuais
        try:
            items = self._get("dca", {
                "an_exercicio": ano - 1, "id_ente": ente, "nr_anexo": 2,
            })
            result["dca"] = items
            self.logger.info("SIAFI/DCA: %d registros", len(items))
        except Exception as e:
            self.logger.warning("Erro SIAFI DCA: %s", e)

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
        with self.db.connect() as conn:
            df.to_sql("execucao_orcamentaria", conn, if_exists="replace", index=False)
        self.logger.info("SIAFI: %d registros na tabela execucao_orcamentaria", len(df))
        return len(df)
