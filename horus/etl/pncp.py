"""ETL do PNCP — Portal Nacional de Contratações Públicas."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import limpar_documento, rate_limiter


class PNCPETL(BaseETL):
    """Extrator de dados do PNCP."""

    nome_fonte = "pncp"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, path: str, params: dict | None = None) -> dict | list:
        rate_limiter.wait("pncp", max_per_minute=60)
        url = f"{self.config.urls.pncp}/{path}"
        resp = self._session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # Modalidades PNCP válidas
    MODALIDADES = {
        "pregao_eletronico": 8,
        "dispensa": 6,
        "concorrencia": 3,
        "tomada_precos": 5,
        "convite": 4,
        "inexigibilidade": 7,
        "leilao": 2,
        "pregao_presencial": 9,
    }

    def extract_contratacoes(self, cnpj_orgao: str = "", cnpj_fornecedor: str = "",
                              data_inicio: str = "", data_fim: str = "",
                              modalidade: int = 8,
                              pagina: int = 1, tam_pagina: int = 50) -> list[dict]:
        # PNCP exige tamanhoPagina >= 10 e codigoModalidadeContratacao obrigatório
        params: dict[str, Any] = {
            "pagina": pagina,
            "tamanhoPagina": max(10, tam_pagina),
            "codigoModalidadeContratacao": modalidade,
        }
        if data_inicio:
            params["dataInicial"] = data_inicio
        if data_fim:
            params["dataFinal"] = data_fim
        if cnpj_orgao:
            params["cnpjOrgao"] = limpar_documento(cnpj_orgao)
        if cnpj_fornecedor:
            params["cnpjFornecedor"] = limpar_documento(cnpj_fornecedor)
        try:
            data = self._get("contratacoes/publicacao", params)
            if isinstance(data, dict):
                return data.get("data", data.get("resultado", [data]))
            return data if isinstance(data, list) else []
        except Exception as e:
            self.logger.warning("Erro PNCP contratações (modalidade=%d): %s", modalidade, e)
            return []

    def extract(self, **kwargs: Any) -> list[dict]:
        cnpj = kwargs.get("cnpj", "")
        data_inicio = kwargs.get("data_inicio", "")
        data_fim = kwargs.get("data_fim", "")
        max_pages = kwargs.get("max_pages", 5)
        modalidades = kwargs.get("modalidades", list(self.MODALIDADES.values()))

        from concurrent.futures import ThreadPoolExecutor

        def _fetch_modalidade(mod_code: int) -> list[dict]:
            items: list[dict] = []
            for page in range(1, max_pages + 1):
                batch = self.extract_contratacoes(
                    cnpj_fornecedor=cnpj, data_inicio=data_inicio,
                    data_fim=data_fim, modalidade=mod_code, pagina=page
                )
                if not batch:
                    break
                items.extend(batch)
            return items

        all_data: list[dict] = []
        with ThreadPoolExecutor(max_workers=min(4, len(modalidades))) as exe:
            for result in exe.map(_fetch_modalidade, modalidades):
                all_data.extend(result)

        return all_data

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        if not raw:
            return pd.DataFrame()

        agora = self._agora()
        rows = []
        for item in raw:
            orgao = item.get("orgaoEntidade", {}) if isinstance(item.get("orgaoEntidade"), dict) else {}
            rows.append({
                "numero": str(item.get("numeroControlePNCP", item.get("numero", ""))),
                "orgao": orgao.get("razaoSocial", item.get("nomeOrgao", "")),
                "orgao_cnpj": limpar_documento(orgao.get("cnpj", item.get("cnpjOrgao", ""))),
                "modalidade": item.get("modalidadeNome", item.get("modalidade", "")),
                "situacao": item.get("situacaoCompra", ""),
                "objeto": item.get("objetoCompra", item.get("descricao", "")),
                "valor_estimado": item.get("valorTotalEstimado", 0),
                "data_abertura": item.get("dataPublicacaoPncp", item.get("dataAbertura", "")),
                "fonte": "pncp",
                "atualizado_em": agora,
            })

        return pd.DataFrame(rows)

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        return self.db.upsert_df("licitacoes", df)
