"""ETL do DataJud — API Pública do CNJ (Conselho Nacional de Justiça)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class DataJudETL(BaseETL):
    """Extrator de dados judiciais via API pública do DataJud/CNJ."""

    nome_fonte = "datajud"

    BASE_URL = "https://api-publica.datajud.cnj.jus.br/api_publica_cnj"

    # Tribunais e classes de interesse para rastreamento político
    TRIBUNAIS = ["tjsp", "tjrj", "tjmg", "tjdf", "stf", "stj"]

    def _search(self, tribunal: str, params: dict) -> dict:
        rate_limiter.wait("datajud", max_per_minute=20)
        url = f"{self.BASE_URL}/_{tribunal}/_search"
        resp = self._session.post(url, json=params, timeout=60, headers={
            "Content-Type": "application/json",
        })
        resp.raise_for_status()
        return resp.json()

    def extract(self, **kwargs: Any) -> list[dict]:
        query = kwargs.get("query", "")
        size = kwargs.get("size", 50)
        tribunais = kwargs.get("tribunais", self.TRIBUNAIS)

        all_hits: list[dict] = []
        body = {
            "size": size,
            "query": {"match_all": {}} if not query else {"match": {"_all": query}},
            "sort": [{"dataAjuizamento": {"order": "desc"}}],
        }

        for tribunal in tribunais:
            try:
                result = self._search(tribunal, body)
                hits = result.get("hits", {}).get("hits", [])
                for h in hits:
                    src = h.get("_source", {})
                    src["_tribunal"] = tribunal
                    all_hits.append(src)
            except Exception as e:
                self.logger.warning("Erro DataJud %s: %s", tribunal, e)

        return all_hits

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame()
        records = []
        for proc in raw:
            records.append({
                "numero": proc.get("numeroProcesso", ""),
                "tribunal": proc.get("_tribunal", ""),
                "classe": proc.get("classe", {}).get("nome", ""),
                "assunto": "; ".join(a.get("nome", "") for a in proc.get("assuntos", [])[:3]),
                "data_ajuizamento": proc.get("dataAjuizamento", ""),
                "orgao_julgador": proc.get("orgaoJulgador", {}).get("nome", ""),
                "grau": proc.get("grau", ""),
            })
        return pd.DataFrame(records)

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "datajud_processos.csv"
        if dest.exists():
            existing = pd.read_csv(dest, dtype=str)
            df = pd.concat([existing, df], ignore_index=True).drop_duplicates(subset=["numero"])
        df.to_csv(dest, index=False)
        return len(df)
