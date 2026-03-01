"""ETL de Sanções da CGU — CEIS, CNEP, CEAF, CEPIM."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import limpar_documento, rate_limiter


class SancoesETL(BaseETL):
    """Extrator de sanções via Portal da Transparência."""

    nome_fonte = "cgu_sancoes"

    ENDPOINTS = {
        "CEIS": "ceis",
        "CNEP": "cnep",
        "CEAF": "ceaf",
        "CEPIM": "cepim",
    }

    def _headers(self) -> dict[str, str]:
        return {
            "chave-api-dados": self.config.transparencia_token,
            "Accept": "application/json",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        rate_limiter.wait("transparencia", max_per_minute=80)
        url = f"{self.config.urls.transparencia}/{endpoint}"
        resp = requests.get(url, headers=self._headers(), params=params or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else [data]

    def _get_paginated(self, endpoint: str, params: dict | None = None, max_pages: int = 50) -> list[dict]:
        params = dict(params or {})
        all_data: list[dict] = []
        for page in range(1, max_pages + 1):
            params["pagina"] = page
            batch = self._get(endpoint, params)
            if not batch:
                break
            all_data.extend(batch)
            if len(batch) < 15:
                break
        return all_data

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        cpf_cnpj = limpar_documento(kwargs.get("cpf_cnpj", ""))
        result: dict[str, list[dict]] = {}

        for tipo, endpoint in self.ENDPOINTS.items():
            params: dict[str, Any] = {}
            if cpf_cnpj:
                if len(cpf_cnpj) == 11:
                    params["cpfSancionado"] = cpf_cnpj
                else:
                    params["cnpjSancionado"] = cpf_cnpj
            try:
                data = self._get_paginated(endpoint, params)
                if data:
                    result[tipo] = data
            except Exception as e:
                self.logger.warning("Erro ao extrair %s: %s", tipo, e)

        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        rows: list[dict] = []
        agora = self._agora()

        for tipo, items in raw.items():
            for item in items:
                sancionado = item.get("sancionado", {}) if isinstance(item.get("sancionado"), dict) else {}
                pessoa = item.get("pessoa", {}) if isinstance(item.get("pessoa"), dict) else {}

                # CNPJ/CPF: tentar vários campos
                cpf_cnpj = ""
                for src in [sancionado, pessoa]:
                    for key in ["cnpjFormatado", "cpfFormatado", "cnpj", "cpf"]:
                        doc = src.get(key, "")
                        if doc:
                            cpf_cnpj = limpar_documento(doc)
                            break
                    if cpf_cnpj:
                        break
                if not cpf_cnpj:
                    cpf_cnpj = limpar_documento(item.get("cpfCnpjSancionado", ""))

                # Nome
                nome = (
                    sancionado.get("nome", "")
                    or pessoa.get("nome", "")
                    or item.get("nomeSancionado", "")
                )

                # Órgão sancionador
                org_sanc = item.get("orgaoSancionador", "")
                if isinstance(org_sanc, dict):
                    org_sanc = org_sanc.get("nome", "")

                # Fundamentação — pode ser lista de dicts ou string
                fund_raw = item.get("fundamentacao", "")
                if isinstance(fund_raw, list):
                    fund = "; ".join(f.get("descricao", f.get("codigo", "")) for f in fund_raw if isinstance(f, dict))[:300]
                elif isinstance(fund_raw, dict):
                    fund = fund_raw.get("descricao", "")
                else:
                    fund = item.get("fundamentacaoLegal", str(fund_raw) if fund_raw else "")

                # UF
                uf = ""
                for src in [sancionado, pessoa]:
                    uf = src.get("uf", "") or src.get("ufSancionado", "")
                    if uf:
                        break
                if not uf:
                    uf = item.get("ufSancionado", "")

                rows.append({
                    "tipo": tipo,
                    "cpf_cnpj": cpf_cnpj,
                    "nome": nome,
                    "orgao_sancionador": org_sanc,
                    "fundamentacao": fund,
                    "data_inicio": item.get("dataInicioSancao", ""),
                    "data_fim": item.get("dataFimSancao", ""),
                    "uf": uf,
                    "fonte": "transparencia",
                    "atualizado_em": agora,
                })

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        return self.db.upsert_df("sancoes", df)
