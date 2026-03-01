"""ETL da Câmara dos Deputados — API Dados Abertos."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import limpar_documento, rate_limiter


class CamaraETL(BaseETL):
    """Extrator de dados da Câmara dos Deputados (dadosabertos.camara.leg.br)."""

    nome_fonte = "camara"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, endpoint: str, params: dict | None = None) -> dict | list:
        rate_limiter.wait("camara", max_per_minute=60)
        url = f"{self.config.urls.camara}/{endpoint}"
        resp = self._session.get(url, params=params or {}, timeout=60,
                                 headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Deputados
    # ------------------------------------------------------------------

    def extract_deputados(self, legislatura: int = 57) -> list[dict]:
        """Extrai lista de todos os deputados da legislatura.
        Legislatura 57 = 2023-2027."""
        self.logger.info("Buscando deputados da legislatura %d...", legislatura)
        data = self._get("deputados", {
            "idLegislatura": legislatura,
            "ordem": "ASC",
            "ordenarPor": "nome",
            "itens": 1000,
        })
        deputados = data.get("dados", []) if isinstance(data, dict) else data
        self.logger.info("Encontrados %d deputados", len(deputados))
        return deputados

    def extract_deputado_detalhe(self, dep_id: int) -> dict:
        """Detalhes de um deputado específico."""
        data = self._get(f"deputados/{dep_id}")
        return data.get("dados", data) if isinstance(data, dict) else data

    # ------------------------------------------------------------------
    # Despesas Parlamentares
    # ------------------------------------------------------------------

    def extract_despesas(self, dep_id: int, ano: int = 2024,
                         max_pages: int = 20) -> list[dict]:
        """Extrai despesas parlamentares de um deputado."""
        all_items: list[dict] = []
        for page in range(1, max_pages + 1):
            data = self._get(f"deputados/{dep_id}/despesas", {
                "ano": ano,
                "itens": 100,
                "pagina": page,
                "ordem": "DESC",
                "ordenarPor": "dataDocumento",
            })
            items = data.get("dados", []) if isinstance(data, dict) else data
            if not items:
                break
            all_items.extend(items)
            if len(items) < 100:
                break
        return all_items

    # ------------------------------------------------------------------
    # Frentes Parlamentares
    # ------------------------------------------------------------------

    def extract_frentes(self, dep_id: int) -> list[dict]:
        """Extrai frentes parlamentares de um deputado."""
        data = self._get(f"deputados/{dep_id}/frentes")
        return data.get("dados", []) if isinstance(data, dict) else data

    # ------------------------------------------------------------------
    # Órgãos (comissões)
    # ------------------------------------------------------------------

    def extract_orgaos(self, dep_id: int) -> list[dict]:
        """Extrai comissões e órgãos de um deputado."""
        data = self._get(f"deputados/{dep_id}/orgaos")
        return data.get("dados", []) if isinstance(data, dict) else data

    # ------------------------------------------------------------------
    # Transform / Interface BaseETL
    # ------------------------------------------------------------------

    def extract(self, **kwargs: Any) -> dict[str, Any]:
        legislatura = kwargs.get("legislatura", 57)
        deps = self.extract_deputados(legislatura)
        return {"deputados": deps, "legislatura": legislatura}

    def transform(self, raw: Any, **kwargs: Any) -> dict[str, pd.DataFrame]:
        agora = self._agora()
        result: dict[str, pd.DataFrame] = {}

        if "deputados" in raw:
            rows = []
            for d in raw["deputados"]:
                rows.append({
                    "id": f"dep_{d['id']}",
                    "id_externo": str(d["id"]),
                    "cpf": "",
                    "nome": d.get("nome", ""),
                    "nome_civil": "",
                    "partido": d.get("siglaPartido", ""),
                    "uf": d.get("siglaUf", ""),
                    "cargo": "Deputado Federal",
                    "legislatura": raw.get("legislatura", 57),
                    "foto_url": d.get("urlFoto", ""),
                    "email": d.get("email", ""),
                    "situacao": "Exercício",
                    "atualizado_em": agora,
                })
            result["politicos"] = pd.DataFrame(rows)

        return result

    def transform_despesas(self, dep_id: int, despesas: list[dict]) -> pd.DataFrame:
        """Transforma despesas brutas em DataFrame."""
        agora = self._agora()
        rows = []
        for d in despesas:
            cnpj = limpar_documento(d.get("cnpjCpfFornecedor", ""))
            rows.append({
                "politico_id": f"dep_{dep_id}",
                "ano": d.get("ano", 0),
                "mes": d.get("mes", 0),
                "tipo": d.get("tipoDespesa", ""),
                "fornecedor_cnpj": cnpj,
                "fornecedor_nome": d.get("nomeFornecedor", ""),
                "valor": d.get("valorDocumento", 0),
                "valor_liquido": d.get("valorLiquido", 0),
                "url_documento": d.get("urlDocumento", ""),
                "atualizado_em": agora,
            })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def load(self, df: Any, **kwargs: Any) -> int:
        total = 0
        if isinstance(df, dict):
            for table, frame in df.items():
                if isinstance(frame, pd.DataFrame) and not frame.empty:
                    total += self.db.upsert_df(table, frame)
        elif isinstance(df, pd.DataFrame):
            total = self.db.upsert_df("despesas_parlamentares", df)
        return total
