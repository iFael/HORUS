"""ETL do INPE — DETER e PRODES (desmatamento via TerraBrasilis WFS).

A API REST /api/v1/alerts retorna 404. O WFS GeoServer funciona (testado 2026-03-01).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from horus.etl.base import BaseETL
from horus.utils import rate_limiter


class INPEETL(BaseETL):
    """Extrator de dados de desmatamento do INPE via WFS GeoServer."""

    nome_fonte = "inpe"

    BASE_URL = "http://terrabrasilis.dpi.inpe.br/geoserver"

    LAYERS = {
        "deter_amazonia": "deter-amz:deter_amz",
        "deter_cerrado": "deter-cerrado:deter_cerrado",
        "prodes_amazonia": "prodes-amz:prodes_amz",
    }

    def _get_wfs(self, layer: str, max_features: int = 500) -> list[dict]:
        """Busca features via WFS GetFeature em GeoJSON."""
        rate_limiter.wait("inpe", max_per_minute=10)
        workspace = layer.split(":")[0]
        url = f"{self.BASE_URL}/{workspace}/ows"
        params = {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": layer,
            "maxFeatures": max_features,
            "outputFormat": "application/json",
        }
        resp = self._session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data.get("features", [])

    def extract(self, **kwargs: Any) -> dict[str, list[dict]]:
        layers = kwargs.get("layers", list(self.LAYERS.keys()))
        max_features = kwargs.get("max_features", 200)
        result: dict[str, list[dict]] = {}
        for nome in layers:
            layer = self.LAYERS.get(nome)
            if not layer:
                continue
            try:
                features = self._get_wfs(layer, max_features)
                result[nome] = features
                self.logger.info("INPE %s: %d features", nome, len(features))
            except Exception as e:
                self.logger.warning("Erro INPE %s: %s", nome, e)
        return result

    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        records = []
        for nome, features in raw.items():
            for feat in features:
                props = feat.get("properties", {})
                props["fonte"] = nome
                # Extrair centroid se houver geometry
                geom = feat.get("geometry")
                if geom and geom.get("coordinates"):
                    coords = geom["coordinates"]
                    if geom.get("type") == "Point":
                        props["longitude"] = coords[0]
                        props["latitude"] = coords[1]
                records.append(props)
        return pd.DataFrame(records) if records else pd.DataFrame()

    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        if df.empty:
            return 0
        with self.db.connect() as conn:
            df.to_sql("desmatamento", conn, if_exists="replace", index=False)
        self.logger.info("INPE: %d registros na tabela desmatamento", len(df))
        return len(df)
