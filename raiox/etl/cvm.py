"""ETL da CVM — Companhias abertas, fundos de investimento."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from raiox.etl.base import BaseETL
from raiox.utils import limpar_documento


class CVMETL(BaseETL):
    """Extrator de dados da CVM (dados abertos)."""

    nome_fonte = "cvm"

    DATASETS = {
        "cia_aberta": "CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv",
        "fundo": "FI/CAD/DADOS/cad_fi.csv",
    }

    def _download(self, path: str, dest: Path) -> Path | None:
        url = f"{self.config.urls.cvm}{path}"
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(resp.content)
            return dest
        except Exception as e:
            self.logger.warning("Erro download CVM %s: %s", path, e)
            return None

    def extract(self, **kwargs: Any) -> dict[str, Path]:
        raw_dir = self.config.paths.raw / "cvm"
        raw_dir.mkdir(parents=True, exist_ok=True)

        result: dict[str, Path] = {}
        for nome, path in self.DATASETS.items():
            fname = path.replace("/", "_")
            dest = raw_dir / fname
            if dest.exists() and dest.stat().st_size > 0:
                result[nome] = dest
            else:
                downloaded = self._download(path, dest)
                if downloaded:
                    result[nome] = downloaded

        return result

    def transform(self, raw: Any, **kwargs: Any) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        agora = self._agora()

        for nome, path in raw.items():
            try:
                df = pd.read_csv(path, sep=";", encoding="latin-1", dtype=str, on_bad_lines="skip")
            except Exception:
                try:
                    df = pd.read_csv(path, sep=",", encoding="utf-8", dtype=str, on_bad_lines="skip")
                except Exception as e:
                    self.logger.warning("Erro lendo CVM %s: %s", nome, e)
                    continue

            df["atualizado_em"] = agora

            if nome == "cia_aberta":
                col_map = {}
                for c in df.columns:
                    cu = c.strip().upper()
                    if "CNPJ" in cu:
                        col_map[c] = "cnpj"
                    elif "DENOM_SOCIAL" in cu or "RAZAO" in cu:
                        col_map[c] = "razao_social"
                    elif "DENOM_COMERC" in cu or "FANTASIA" in cu:
                        col_map[c] = "nome_fantasia"
                    elif "SIT" in cu:
                        col_map[c] = "situacao"
                    elif "DT_REG" in cu:
                        col_map[c] = "data_abertura"
                df = df.rename(columns=col_map)
                if "cnpj" in df.columns:
                    df["cnpj"] = df["cnpj"].apply(limpar_documento)
                    result["empresas_cvm"] = df

            elif nome == "fundo":
                col_map = {}
                for c in df.columns:
                    cu = c.strip().upper()
                    if "CNPJ_FUNDO" in cu:
                        col_map[c] = "cnpj"
                    elif "DENOM_SOCIAL" in cu:
                        col_map[c] = "razao_social"
                    elif "SIT" in cu:
                        col_map[c] = "situacao"
                    elif "DT_REG" in cu:
                        col_map[c] = "data_abertura"
                    elif "ADMIN" in cu and "CNPJ" in cu:
                        col_map[c] = "cnpj_admin"
                df = df.rename(columns=col_map)
                result["fundos_cvm"] = df

        return result

    def load(self, df: Any, **kwargs: Any) -> int:
        total = 0
        if not isinstance(df, dict):
            return 0

        if "empresas_cvm" in df and not df["empresas_cvm"].empty:
            empresas = df["empresas_cvm"]
            cols = ["cnpj", "razao_social", "nome_fantasia", "situacao", "data_abertura", "atualizado_em"]
            existing = [c for c in cols if c in empresas.columns]
            if "cnpj" in existing:
                total += self.db.upsert_df("empresas", empresas[existing])

        for key in ["fundos_cvm"]:
            if key in df and not df[key].empty:
                dest = self.config.paths.processed / f"{key}.csv"
                df[key].to_csv(dest, index=False)
                total += len(df[key])

        return total
