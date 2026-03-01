"""ETL da Receita Federal — CNPJ, QSA (Quadro Societário) via dados abertos."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm

from raiox.etl.base import BaseETL
from raiox.utils import limpar_documento


class ReceitaCNPJETL(BaseETL):
    """Extrator de dados do CNPJ/QSA da Receita Federal (arquivos ZIP)."""

    nome_fonte = "receita_cnpj"

    # Prefixos dos arquivos na Receita
    PREFIXOS = {
        "empresas": "Empresas",
        "socios": "Socios",
        "estabelecimentos": "Estabelecimentos",
    }

    def _download_file(self, url: str, dest: Path) -> Path:
        """Download com barra de progresso."""
        if dest.exists() and dest.stat().st_size > 0:
            self.logger.info("Arquivo já existe: %s", dest.name)
            return dest

        self.logger.info("Baixando %s ...", url)
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as pbar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))
        return dest

    def _list_remote_files(self, prefixo: str) -> list[str]:
        """Lista arquivos ZIP disponíveis na página da Receita."""
        try:
            resp = requests.get(self.config.urls.receita_cnpj, timeout=120)
            resp.raise_for_status()
            import re
            pattern = rf'href="({prefixo}\d+\.zip)"'
            matches = re.findall(pattern, resp.text, re.IGNORECASE)
            return [f"{self.config.urls.receita_cnpj}{m}" for m in matches]
        except Exception as e:
            self.logger.warning("Erro ao listar arquivos Receita: %s", e)
            return []

    def _read_zip_csv(self, zip_path: Path, encoding: str = "latin-1", sep: str = ";") -> pd.DataFrame:
        """Lê CSV dentro de arquivo ZIP."""
        frames = []
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                if name.endswith(".csv") or name.endswith(".CSV") or "." not in name.split("/")[-1]:
                    with zf.open(name) as f:
                        try:
                            df = pd.read_csv(
                                io.TextIOWrapper(f, encoding=encoding),
                                sep=sep,
                                dtype=str,
                                on_bad_lines="skip",
                                header=None,
                            )
                            frames.append(df)
                        except Exception as e:
                            self.logger.warning("Erro lendo %s/%s: %s", zip_path.name, name, e)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def extract(self, **kwargs: Any) -> dict[str, list[Path]]:
        """Baixa arquivos ZIP da Receita. Retorna paths locais."""
        raw_dir = self.config.paths.raw / "receita_cnpj"
        raw_dir.mkdir(parents=True, exist_ok=True)
        downloaded: dict[str, list[Path]] = {}

        max_files = kwargs.get("max_files", 1)  # limitar para teste

        for tipo, prefixo in self.PREFIXOS.items():
            urls = self._list_remote_files(prefixo)
            paths = []
            for url in urls[:max_files]:
                fname = url.split("/")[-1]
                dest = raw_dir / fname
                try:
                    self._download_file(url, dest)
                    paths.append(dest)
                except Exception as e:
                    self.logger.warning("Erro download %s: %s", fname, e)
            downloaded[tipo] = paths

        return downloaded

    def transform(self, raw: Any, **kwargs: Any) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        agora = self._agora()

        # Empresas
        for path in raw.get("empresas", []):
            df = self._read_zip_csv(path)
            if df.empty:
                continue
            # Colunas padrão Receita (sem header)
            col_map = {0: "cnpj_basico", 1: "razao_social", 2: "natureza_juridica",
                       3: "qualificacao_responsavel", 4: "capital_social", 5: "porte"}
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if "cnpj_basico" in df.columns:
                df["cnpj"] = df["cnpj_basico"].str.strip()
                df["atualizado_em"] = agora
                result["empresas"] = df

        # Sócios
        for path in raw.get("socios", []):
            df = self._read_zip_csv(path)
            if df.empty:
                continue
            col_map = {0: "cnpj_basico", 1: "tipo_socio", 2: "nome_socio",
                       3: "cpf_cnpj_socio", 4: "qualificacao", 5: "data_entrada"}
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if "cnpj_basico" in df.columns:
                df["cnpj"] = df["cnpj_basico"].str.strip()
                df["atualizado_em"] = agora
                result["socios"] = df

        # Estabelecimentos (para endereço, UF, etc.)
        for path in raw.get("estabelecimentos", []):
            df = self._read_zip_csv(path)
            if df.empty:
                continue
            col_map = {
                0: "cnpj_basico", 1: "cnpj_ordem", 2: "cnpj_dv",
                3: "matriz_filial", 4: "nome_fantasia", 5: "situacao",
                6: "data_situacao", 7: "motivo_situacao", 8: "nome_cidade_exterior",
                9: "pais", 10: "data_abertura", 11: "cnae_principal",
                12: "cnae_secundario", 13: "tipo_logradouro", 14: "logradouro",
                15: "numero", 16: "complemento", 17: "bairro", 18: "cep",
                19: "uf", 20: "municipio",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if "cnpj_basico" in df.columns:
                df["atualizado_em"] = agora
                result["estabelecimentos"] = df

        return result

    def load(self, df: Any, **kwargs: Any) -> int:
        total = 0
        if not isinstance(df, dict):
            return 0

        agora = self._agora()

        if "empresas" in df and not df["empresas"].empty:
            empresas = df["empresas"]
            cols_db = ["cnpj", "razao_social", "natureza_juridica", "capital_social", "porte", "atualizado_em"]
            existing = [c for c in cols_db if c in empresas.columns]
            if existing:
                total += self.db.upsert_df("empresas", empresas[existing])

        if "socios" in df and not df["socios"].empty:
            socios = df["socios"]
            cols_db = ["cnpj", "tipo_socio", "cpf_cnpj_socio", "nome_socio", "qualificacao", "data_entrada", "atualizado_em"]
            existing = [c for c in cols_db if c in socios.columns]
            if existing:
                total += self.db.upsert_df("socios", socios[existing])

        if "estabelecimentos" in df and not df["estabelecimentos"].empty:
            estab = df["estabelecimentos"]
            # Atualizar empresas com dados de endereço
            if all(c in estab.columns for c in ["cnpj_basico", "uf", "municipio"]):
                for _, row in estab.head(1000).iterrows():  # limitar para performance
                    cnpj_base = str(row.get("cnpj_basico", "")).strip()
                    if cnpj_base:
                        self.db.query(
                            "UPDATE empresas SET uf=?, municipio=?, cep=?, data_abertura=?, "
                            "cnae_principal=?, situacao=?, atualizado_em=? "
                            "WHERE cnpj LIKE ?",
                            (
                                row.get("uf", ""), row.get("municipio", ""),
                                row.get("cep", ""), row.get("data_abertura", ""),
                                row.get("cnae_principal", ""), row.get("situacao", ""),
                                agora, f"{cnpj_base}%",
                            ),
                        )

        return total
