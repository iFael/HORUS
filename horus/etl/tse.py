"""ETL do TSE — Candidaturas, Bens, Doações de campanha."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

from horus.etl.base import BaseETL
from horus.utils import limpar_documento, normalizar_nome


class TSEETL(BaseETL):
    """Extrator de dados do TSE (dados abertos)."""

    nome_fonte = "tse"

    # URLs base para datasets
    DATASETS = {
        "candidatos": "candidatos-{ano}",
        "bens": "bem-candidato-{ano}",
        "receitas": "prestacao-de-contas-eleitorais-candidatos-{ano}",
        "resultados": "resultados-{ano}",
    }

    def _download_dataset(self, dataset_id: str, raw_dir: Path) -> Path | None:
        """Tenta baixar dataset do portal TSE CKAN."""
        base_url = self.config.urls.tse
        # Tentar formato CKAN
        api_url = f"https://dadosabertos.tse.jus.br/api/3/action/package_show?id={dataset_id}"
        try:
            resp = self._session.get(api_url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                resources = data.get("result", {}).get("resources", [])
                for r in resources:
                    url = r.get("url", "")
                    if url.endswith(".zip") or url.endswith(".csv"):
                        fname = url.split("/")[-1]
                        dest = raw_dir / fname
                        if dest.exists() and dest.stat().st_size > 0:
                            return dest
                        self.logger.info("Baixando TSE: %s", fname)
                        dl = self._session.get(url, stream=True, timeout=120)
                        dl.raise_for_status()
                        with open(dest, "wb") as f:
                            for chunk in dl.iter_content(8192):
                                f.write(chunk)
                        return dest
        except Exception as e:
            self.logger.warning("Erro ao baixar dataset TSE %s: %s", dataset_id, e)
        return None

    def extract(self, **kwargs: Any) -> dict[str, list[Path]]:
        ano = kwargs.get("ano", 2022)
        raw_dir = self.config.paths.raw / "tse"
        raw_dir.mkdir(parents=True, exist_ok=True)

        result: dict[str, list[Path]] = {}
        for tipo, template in self.DATASETS.items():
            dataset_id = template.format(ano=ano)
            path = self._download_dataset(dataset_id, raw_dir)
            if path:
                result[tipo] = [path]

        return result

    def _read_tse_csv(self, path: Path) -> pd.DataFrame:
        """Lê CSV do TSE (encoding latin-1, sep ;)."""
        try:
            if path.suffix.lower() == ".zip":
                frames = []
                with zipfile.ZipFile(path) as zf:
                    for name in zf.namelist():
                        if name.endswith(".csv"):
                            with zf.open(name) as f:
                                df = pd.read_csv(
                                    io.TextIOWrapper(f, encoding="latin-1"),
                                    sep=";", dtype=str, on_bad_lines="skip",
                                )
                                frames.append(df)
                return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            else:
                return pd.read_csv(path, sep=";", encoding="latin-1", dtype=str, on_bad_lines="skip")
        except Exception as e:
            self.logger.warning("Erro lendo TSE CSV %s: %s", path, e)
            return pd.DataFrame()

    def transform(self, raw: Any, **kwargs: Any) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        agora = self._agora()

        # Candidatos
        for path in raw.get("candidatos", []):
            df = self._read_tse_csv(path)
            if df.empty:
                continue
            # Colunas comuns do TSE (podem variar por ano)
            col_map = {}
            for c in df.columns:
                cl = c.strip().upper()
                if "NR_CPF" in cl or "CPF" == cl:
                    col_map[c] = "cpf"
                elif "NM_CANDIDATO" in cl or "NOME_CANDIDATO" in cl:
                    col_map[c] = "nome"
                elif "ANO_ELEICAO" in cl:
                    col_map[c] = "ano_eleicao"
                elif "DS_CARGO" in cl or "CARGO" == cl:
                    col_map[c] = "cargo"
                elif "SG_PARTIDO" in cl:
                    col_map[c] = "partido"
                elif "SG_UF" in cl:
                    col_map[c] = "uf"
                elif "NM_UE" in cl or "MUNICIPIO" in cl:
                    col_map[c] = "municipio"
                elif "DS_SIT_TOT" in cl or "SITUACAO" in cl:
                    col_map[c] = "situacao"
                elif "QT_VOTOS" in cl:
                    col_map[c] = "votos"

            df = df.rename(columns=col_map)
            if "cpf" in df.columns:
                df["cpf"] = df["cpf"].apply(limpar_documento)
                df["atualizado_em"] = agora
                result["candidaturas"] = df

        # Receitas (doações)
        for path in raw.get("receitas", []):
            df = self._read_tse_csv(path)
            if df.empty:
                continue
            col_map = {}
            for c in df.columns:
                cl = c.strip().upper()
                if "CPF_CNPJ_DOADOR" in cl:
                    col_map[c] = "cpf_cnpj_doador"
                elif "NM_DOADOR" in cl or "NOME_DOADOR" in cl:
                    col_map[c] = "nome_doador"
                elif "NR_CPF_CANDIDATO" in cl:
                    col_map[c] = "cpf_candidato"
                elif "NM_CANDIDATO" in cl:
                    col_map[c] = "nome_candidato"
                elif "ANO_ELEICAO" in cl:
                    col_map[c] = "ano_eleicao"
                elif "VR_RECEITA" in cl or "VALOR" in cl:
                    col_map[c] = "valor"
                elif "DS_FONTE_RECEITA" in cl:
                    col_map[c] = "tipo_recurso"
                elif "SG_PARTIDO" in cl:
                    col_map[c] = "partido"

            df = df.rename(columns=col_map)
            if "cpf_cnpj_doador" in df.columns:
                df["cpf_cnpj_doador"] = df["cpf_cnpj_doador"].apply(limpar_documento)
                df["atualizado_em"] = agora
                result["doacoes"] = df

        return result

    def load(self, df: Any, **kwargs: Any) -> int:
        total = 0
        if not isinstance(df, dict):
            return 0

        if "candidaturas" in df and not df["candidaturas"].empty:
            cand = df["candidaturas"]
            cols = ["cpf", "nome", "ano_eleicao", "cargo", "partido", "uf",
                    "municipio", "situacao", "votos", "atualizado_em"]
            existing = [c for c in cols if c in cand.columns]
            if "cpf" in existing:
                total += self.db.upsert_df("candidaturas", cand[existing])

        if "doacoes" in df and not df["doacoes"].empty:
            doac = df["doacoes"]
            cols = ["cpf_cnpj_doador", "nome_doador", "cpf_candidato",
                    "nome_candidato", "ano_eleicao", "valor", "tipo_recurso",
                    "partido", "atualizado_em"]
            existing = [c for c in cols if c in doac.columns]
            if "cpf_cnpj_doador" in existing:
                total += self.db.upsert_df("doacoes", doac[existing])

        return total
