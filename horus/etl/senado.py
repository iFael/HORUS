"""ETL do Senado Federal — API Dados Abertos."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class SenadoETL(BaseETL):
    """Extrator de dados do Senado Federal (legis.senado.leg.br/dadosabertos)."""

    nome_fonte = "senado"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        rate_limiter.wait("senado", max_per_minute=60)
        url = f"{self.config.urls.senado}/{endpoint}"
        resp = requests.get(url, params=params or {}, timeout=60,
                            headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Senadores
    # ------------------------------------------------------------------

    def extract_senadores_atuais(self) -> list[dict]:
        """Extrai lista de todos os senadores em exercício."""
        self.logger.info("Buscando senadores em exercício...")
        data = self._get("senador/lista/atual.json")

        # Estrutura: ListaParlamentarEmExercicio.Parlamentares.Parlamentar
        container = data.get("ListaParlamentarEmExercicio", {})
        parlamentares = container.get("Parlamentares", {})
        senadores = parlamentares.get("Parlamentar", [])

        if isinstance(senadores, dict):
            senadores = [senadores]

        self.logger.info("Encontrados %d senadores", len(senadores))
        return senadores

    def extract_senador_detalhe(self, codigo: str) -> dict:
        """Detalhes de um senador específico."""
        data = self._get(f"senador/{codigo}.json")
        return data.get("DetalheParlamentar", {}).get("Parlamentar", data)

    # ------------------------------------------------------------------
    # Interface BaseETL
    # ------------------------------------------------------------------

    def extract(self, **kwargs: Any) -> dict[str, Any]:
        senadores = self.extract_senadores_atuais()
        return {"senadores": senadores}

    def transform(self, raw: Any, **kwargs: Any) -> dict[str, pd.DataFrame]:
        agora = self._agora()
        result: dict[str, pd.DataFrame] = {}

        if "senadores" in raw:
            rows = []
            for s in raw["senadores"]:
                ident = s.get("IdentificacaoParlamentar", {})
                mandato = s.get("Mandatos", {}).get("Mandato", {})
                if isinstance(mandato, list):
                    mandato = mandato[0] if mandato else {}

                rows.append({
                    "id": f"sen_{ident.get('CodigoParlamentar', '')}",
                    "id_externo": str(ident.get("CodigoParlamentar", "")),
                    "cpf": "",
                    "nome": ident.get("NomeParlamentar", ""),
                    "nome_civil": ident.get("NomeCompletoParlamentar", ""),
                    "partido": ident.get("SiglaPartidoParlamentar", ""),
                    "uf": ident.get("UfParlamentar", ""),
                    "cargo": "Senador",
                    "legislatura": 57,
                    "foto_url": ident.get("UrlFotoParlamentar", ""),
                    "email": ident.get("EmailParlamentar", ""),
                    "situacao": "Exercício",
                    "atualizado_em": agora,
                })
            result["politicos"] = pd.DataFrame(rows)

        return result

    def load(self, df: Any, **kwargs: Any) -> int:
        total = 0
        if isinstance(df, dict):
            for table, frame in df.items():
                if isinstance(frame, pd.DataFrame) and not frame.empty:
                    total += self.db.upsert_df(table, frame)
        return total
