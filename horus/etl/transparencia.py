"""ETL do Portal da Transparência — servidores, contratos, licitações, emendas."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import limpar_documento, rate_limiter


def _parse_valor(v: Any) -> float:
    """Converte valor da API para float. API pode retornar string BR ('2.550,00') ou numérico."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    # Formato BR: "1.234.567,89" → remover pontos de milhar, trocar vírgula por ponto
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


class TransparenciaETL(BaseETL):
    """Extractor para API do Portal da Transparência (CGU)."""

    nome_fonte = "transparencia"

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

    def _get_paginated(
        self, endpoint: str, params: dict | None = None, max_pages: int = 50
    ) -> list[dict]:
        """Busca paginada. A API usa ?pagina=N."""
        params = dict(params or {})
        all_data: list[dict] = []
        for page in range(1, max_pages + 1):
            params["pagina"] = page
            batch = self._get(endpoint, params)
            if not batch:
                break
            all_data.extend(batch)
            if len(batch) < 15:  # página padrão = 15 itens
                break
        return all_data

    # ------------------------------------------------------------------
    # Servidores
    # ------------------------------------------------------------------
    def extract_servidores(self, cpf: str = "", codigo_orgao: str = "") -> list[dict]:
        """Busca servidores. Endpoint requer cpf OU codigoOrgaoExercicio/Lotacao.
        
        NOTA: 'servidores/por-nome' e 'servidores/por-cpf' são APIs restritas (403).
        Usar o endpoint base 'servidores' com param 'cpf' (lowercase).
        """
        params: dict[str, Any] = {}
        if cpf:
            params["cpf"] = limpar_documento(cpf)
        elif codigo_orgao:
            params["codigoOrgaoExercicio"] = codigo_orgao
        else:
            return []
        return self._get_paginated("servidores", params)

    # ------------------------------------------------------------------
    # Contratos
    # ------------------------------------------------------------------

    def extract_contratos(self, codigo_orgao: str = "", cnpj_fornecedor: str = "") -> list[dict]:
        """Busca contratos. Requer codigoOrgao obrigatório."""
        params: dict[str, Any] = {}
        if codigo_orgao:
            params["codigoOrgao"] = codigo_orgao
        elif cnpj_fornecedor:
            # API exige codigoOrgao; se só tiver fornecedor, não é possível buscar
            return []
        else:
            return []
        if cnpj_fornecedor:
            params["cnpjFornecedor"] = limpar_documento(cnpj_fornecedor)
        return self._get_paginated("contratos", params)

    # ------------------------------------------------------------------
    # Licitações
    # ------------------------------------------------------------------

    def extract_licitacoes(
        self, codigo_orgao: str = "",
        data_inicial: str = "", data_final: str = ""
    ) -> list[dict]:
        """Busca licitações. Requer codigoOrgao + período (max 1 mês)."""
        params: dict[str, Any] = {}
        if not codigo_orgao:
            return []
        params["codigoOrgao"] = codigo_orgao
        if data_inicial:
            params["dataInicial"] = data_inicial
        if data_final:
            params["dataFinal"] = data_final
        return self._get_paginated("licitacoes", params)

    # ------------------------------------------------------------------
    # Emendas
    # ------------------------------------------------------------------

    def extract_emendas(self, ano: int = 0, cpf_autor: str = "") -> list[dict]:
        """Busca emendas parlamentares. Endpoint 'emendas' (não 'emendas-parlamentares' que é 403)."""
        params: dict[str, Any] = {}
        if ano:
            params["ano"] = ano
        if cpf_autor:
            params["cpfAutor"] = limpar_documento(cpf_autor)
        if not params:
            return []
        return self._get_paginated("emendas", params)

    # ------------------------------------------------------------------
    # Interface BaseETL
    # ------------------------------------------------------------------

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        """Extrai todos os tipos de dados relevantes."""
        data: dict[str, list[dict]] = {}
        cpf = kwargs.get("cpf", "")
        nome = kwargs.get("nome", "")  # NOTE: busca por nome não é mais suportada (API restrita)
        cnpj = kwargs.get("cnpj", "")
        codigo_orgao = kwargs.get("codigo_orgao", "")

        if cpf:
            data["servidores"] = self.extract_servidores(cpf=cpf)

        if codigo_orgao:
            data["contratos"] = self.extract_contratos(codigo_orgao=codigo_orgao)
            data["licitacoes"] = self.extract_licitacoes(
                codigo_orgao=codigo_orgao,
                data_inicial=kwargs.get("data_inicial", ""),
                data_final=kwargs.get("data_final", ""),
            )

        if cpf:
            data["emendas"] = self.extract_emendas(cpf_autor=cpf)

        return data

    def transform(self, raw: Any, **kwargs: Any) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        agora = self._agora()

        if "servidores" in raw and raw["servidores"]:
            rows = []
            for s in raw["servidores"]:
                rows.append({
                    "cpf": limpar_documento(s.get("cpf", s.get("cpfServidor", ""))),
                    "nome": s.get("nome", ""),
                    "orgao": s.get("orgaoServidorExercicio", {}).get("nome", "")
                             if isinstance(s.get("orgaoServidorExercicio"), dict)
                             else s.get("orgaoExercicio", ""),
                    "orgao_cnpj": "",
                    "cargo": s.get("cargo", {}).get("nome", "")
                             if isinstance(s.get("cargo"), dict) else s.get("cargo", ""),
                    "funcao": s.get("funcao", {}).get("nome", "")
                              if isinstance(s.get("funcao"), dict) else s.get("funcao", ""),
                    "remuneracao": s.get("remuneracaoBasicaBruta", 0),
                    "data_ingresso": s.get("dataIngressoOrgao", ""),
                    "situacao_vinculo": s.get("situacaoVinculo", ""),
                    "uf": s.get("ufExercicio", ""),
                    "atualizado_em": agora,
                })
            result["servidores"] = pd.DataFrame(rows)

        if "contratos" in raw and raw["contratos"]:
            rows = []
            for c in raw["contratos"]:
                forn = c.get("fornecedor", {}) if isinstance(c.get("fornecedor"), dict) else {}
                ug = c.get("unidadeGestora", {}) if isinstance(c.get("unidadeGestora"), dict) else {}
                ov = ug.get("orgaoVinculado", {}) if isinstance(ug.get("orgaoVinculado"), dict) else {}
                mc = c.get("modalidadeCompra", {}) if isinstance(c.get("modalidadeCompra"), dict) else {}
                # API retorna cnpjFormatado ("05.504.370/0001-80") e valorInicialCompra
                cnpj_raw = forn.get("cnpjFormatado", "") or forn.get("cnpj", "") or forn.get("cpfFormatado", "")
                valor_raw = c.get("valorInicialCompra", 0) or c.get("valorInicial", 0) or c.get("valorFinalCompra", 0)
                rows.append({
                    "numero": str(c.get("id", "") or c.get("numero", "")),
                    "orgao": ov.get("nome", "") or ug.get("nome", ""),
                    "orgao_cnpj": limpar_documento(ov.get("cnpj", "")),
                    "fornecedor_cnpj": limpar_documento(cnpj_raw),
                    "fornecedor_nome": forn.get("nome", ""),
                    "objeto": c.get("objeto", ""),
                    "valor": _parse_valor(valor_raw),
                    "data_inicio": c.get("dataInicioVigencia", ""),
                    "data_fim": c.get("dataFimVigencia", ""),
                    "modalidade": mc.get("descricao", "") if isinstance(mc, dict) else str(mc),
                    "fonte": "transparencia",
                    "atualizado_em": agora,
                })
            result["contratos"] = pd.DataFrame(rows)

        if "licitacoes" in raw and raw["licitacoes"]:
            rows = []
            for l in raw["licitacoes"]:
                rows.append({
                    "numero": l.get("numero", ""),
                    "orgao": l.get("unidadeGestora", {}).get("orgaoVinculado", {}).get("nome", "")
                             if isinstance(l.get("unidadeGestora"), dict) else "",
                    "orgao_cnpj": "",
                    "modalidade": l.get("modalidadeLicitacao", {}).get("descricao", "")
                                  if isinstance(l.get("modalidadeLicitacao"), dict) else "",
                    "situacao": l.get("situacao", {}).get("descricao", "")
                                if isinstance(l.get("situacao"), dict) else "",
                    "objeto": l.get("objeto", ""),
                    "valor_estimado": l.get("valorLicitacao", 0),
                    "data_abertura": l.get("dataAbertura", ""),
                    "fonte": "transparencia",
                    "atualizado_em": agora,
                })
            result["licitacoes"] = pd.DataFrame(rows)

        if "emendas" in raw and raw["emendas"]:
            rows = []
            for e in raw["emendas"]:
                # API retorna localidadeDoGasto como string, ex: "AMAPÁ (UF)"
                loc_raw = e.get("localidadeDoGasto", "")
                if isinstance(loc_raw, dict):
                    loc_nome = loc_raw.get("nome", "")
                    loc_uf = loc_raw.get("uf", "")
                else:
                    loc_nome = str(loc_raw) if loc_raw else ""
                    # Tentar extrair UF do padrão "ESTADO (UF)"
                    loc_uf = ""
                    if "(" in loc_nome:
                        loc_uf = loc_nome.split("(")[-1].replace(")", "").strip()[:2]
                
                # Autor pode ser string direta ou objeto
                autor_raw = e.get("autor", "")
                if isinstance(autor_raw, dict):
                    autor_nome = autor_raw.get("nome", "")
                    autor_cpf = limpar_documento(autor_raw.get("cpf", ""))
                else:
                    autor_nome = e.get("nomeAutor", "") or str(autor_raw)
                    autor_cpf = ""

                # Função/subfunção podem ser string ou dict
                funcao = e.get("funcao", "")
                if isinstance(funcao, dict):
                    funcao = funcao.get("nome", "")
                subfuncao = e.get("subfuncao", "")
                if isinstance(subfuncao, dict):
                    subfuncao = subfuncao.get("nome", "")

                rows.append({
                    "numero": str(e.get("codigoEmenda", "") or e.get("numero", "")),
                    "autor": autor_nome,
                    "autor_cpf": autor_cpf,
                    "tipo": e.get("tipoEmenda", e.get("tipo", "")),
                    "ano": e.get("ano", 0),
                    "valor_empenhado": _parse_valor(e.get("valorEmpenhado", 0)),
                    "valor_pago": _parse_valor(e.get("valorPago", 0)),
                    "localidade": loc_nome,
                    "uf": loc_uf,
                    "funcao": funcao,
                    "subfuncao": subfuncao,
                    "atualizado_em": agora,
                })
            result["emendas"] = pd.DataFrame(rows)

        return result  # type: ignore[return-value]

    def load(self, df: Any, **kwargs: Any) -> int:
        """df aqui é dict[str, DataFrame]."""
        total = 0
        if isinstance(df, dict):
            for table, frame in df.items():
                if isinstance(frame, pd.DataFrame) and not frame.empty:
                    total += self.db.upsert_df(table, frame)
        return total
