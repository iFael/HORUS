"""Classe base abstrata para todos os módulos ETL."""

from __future__ import annotations

import abc
from datetime import datetime
from typing import Any

import pandas as pd

from raiox.config import Config
from raiox.database import DatabaseManager
from raiox.utils import get_logger


class BaseETL(abc.ABC):
    """Interface padrão para módulos de extração, transformação e carga."""

    nome_fonte: str = "base"

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self.config = config or db.config
        self.logger = get_logger(f"etl.{self.nome_fonte}", self.config.log_level)

    # ------------------------------------------------------------------
    # Template Method
    # ------------------------------------------------------------------

    def run(self, force: bool = False, **kwargs: Any) -> int:
        """Executa pipeline completo. Retorna total de registros carregados."""
        cache_key = self._cache_key(**kwargs)
        if not force and self.db.cache_valido(cache_key):
            self.logger.info("Cache válido para '%s', pulando.", cache_key)
            return 0

        self.logger.info("Iniciando ETL: %s", self.nome_fonte)
        raw = self.extract(**kwargs)
        if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
            self.logger.warning("Nenhum dado extraído para '%s'", cache_key)
            return 0

        df = self.transform(raw, **kwargs)
        count = self.load(df, **kwargs)

        self.db.atualizar_cache(cache_key, self.nome_fonte, count)
        self.logger.info("ETL %s concluído: %d registros", self.nome_fonte, count)
        return count

    # ------------------------------------------------------------------
    # Métodos abstratos
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def extract(self, **kwargs: Any) -> Any:
        """Extrai dados brutos da fonte."""

    @abc.abstractmethod
    def transform(self, raw: Any, **kwargs: Any) -> pd.DataFrame:
        """Transforma os dados brutos em DataFrame padronizado."""

    @abc.abstractmethod
    def load(self, df: pd.DataFrame, **kwargs: Any) -> int:
        """Carrega DataFrame no banco. Retorna registros inseridos."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cache_key(self, **kwargs: Any) -> str:
        parts = [self.nome_fonte] + [f"{k}={v}" for k, v in sorted(kwargs.items())]
        return "|".join(parts)

    @staticmethod
    def _agora() -> str:
        return datetime.now().isoformat()
