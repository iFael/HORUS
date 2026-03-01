"""ETL do DATASUS — dados de saúde pública."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import requests

from raiox.etl.base import BaseETL


class DATASUSETL(BaseETL):
    """Extrator de dados do DATASUS (FTP/download)."""

    nome_fonte = "datasus"

    # URLs fixas de datasets conhecidos
    DATASETS = {
        "cnes_estabelecimentos": "https://dadosabertos.saude.gov.br/dataset/cnes-dados-abertos",
    }

    def extract(self, **kwargs: Any) -> dict[str, Path]:
        """Baixa datasets do DATASUS. Retorna paths locais."""
        raw_dir = self.config.paths.raw / "datasus"
        raw_dir.mkdir(parents=True, exist_ok=True)

        result: dict[str, Path] = {}
        datasets = kwargs.get("datasets", list(self.DATASETS.keys()))

        for nome in datasets:
            url = self.DATASETS.get(nome, "")
            if not url:
                continue
            dest = raw_dir / f"{nome}.html"
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                dest.write_text(resp.text, encoding="utf-8")
                result[nome] = dest
            except Exception as e:
                self.logger.warning("Erro DATASUS %s: %s", nome, e)

        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        # DATASUS requer processamento específico por dataset
        # Aqui implementamos estrutura base
        return pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        dest = self.config.paths.processed / "datasus_dados.csv"
        df.to_csv(dest, index=False)
        return len(df)
