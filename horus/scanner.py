"""Scanner Automático de Políticos — Coleta e enriquecimento de dados."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import pandas as pd

from horus.config import Config
from horus.database import DatabaseManager
from horus.utils import get_logger

logger = get_logger(__name__)


class PoliticianScanner:
    """Pipeline automático: descobre políticos → enriquece dados → detecta anomalias."""

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self.config = config or db.config

    # ------------------------------------------------------------------
    # 1. Descoberta de políticos
    # ------------------------------------------------------------------

    def discover(self) -> int:
        """Busca todos os deputados e senadores e salva no banco.
        Retorna total de políticos encontrados."""
        total = 0

        # Câmara dos Deputados
        try:
            from horus.etl.camara import CamaraETL
            camara = CamaraETL(self.db)
            raw = camara.extract(legislatura=57)
            transformed = camara.transform(raw)
            if "politicos" in transformed:
                n = camara.load(transformed)
                total += len(transformed["politicos"])
                logger.info("Câmara: %d deputados carregados", len(transformed["politicos"]))
        except Exception as e:
            logger.error("Erro ao carregar deputados: %s", e)

        # Senado Federal
        try:
            from horus.etl.senado import SenadoETL
            senado = SenadoETL(self.db)
            raw = senado.extract()
            transformed = senado.transform(raw)
            if "politicos" in transformed:
                n = senado.load(transformed)
                total += len(transformed["politicos"])
                logger.info("Senado: %d senadores carregados", len(transformed["politicos"]))
        except Exception as e:
            logger.error("Erro ao carregar senadores: %s", e)

        return total

    # ------------------------------------------------------------------
    # 2. Enriquecimento de dados
    # ------------------------------------------------------------------

    def enrich_despesas(self, anos: list[int] | None = None, max_deputados: int = 600) -> int:
        """Busca despesas parlamentares de todos os deputados.
        Retorna total de registros de despesas."""
        if anos is None:
            anos = [datetime.now().year - 1, datetime.now().year]

        from horus.etl.camara import CamaraETL
        camara = CamaraETL(self.db)
        politicos = self.db.buscar_politicos(cargo="Deputado Federal", limite=max_deputados)
        total = 0

        for i, pol in enumerate(politicos):
            dep_id = int(pol["id_externo"])
            logger.info("[%d/%d] Despesas de %s...", i + 1, len(politicos), pol["nome"])
            for ano in anos:
                try:
                    raw = camara.extract_despesas(dep_id, ano)
                    if raw:
                        df = camara.transform_despesas(dep_id, raw)
                        if not df.empty:
                            camara.load(df)
                            total += len(df)
                except Exception as e:
                    logger.warning("Erro despesas %s/%d: %s", pol["nome"], ano, e)

        logger.info("Total despesas coletadas: %d", total)
        return total

    def enrich_emendas(self, anos: list[int] | None = None) -> int:
        """Busca emendas parlamentares via Portal da Transparência.
        Retorna total de emendas."""
        if anos is None:
            anos = [datetime.now().year - 1, datetime.now().year]

        from horus.etl.transparencia import TransparenciaETL
        transp = TransparenciaETL(self.db)
        total = 0

        for ano in anos:
            try:
                logger.info("Buscando emendas do ano %d...", ano)
                raw_emendas = transp.extract_emendas(ano=ano)
                if raw_emendas:
                    transformed = transp.transform({"emendas": raw_emendas})
                    if "emendas" in transformed and not transformed["emendas"].empty:
                        n = self.db.upsert_df("emendas", transformed["emendas"])
                        total += n
                        logger.info("Ano %d: %d emendas", ano, n)
            except Exception as e:
                logger.warning("Erro emendas %d: %s", ano, e)

        return total

    def enrich_contratos(self, codigos_orgao: list[str] | None = None) -> int:
        """Busca contratos via Portal da Transparência.
        Se nenhum código fornecido, usa órgãos federais comuns."""
        if codigos_orgao is None:
            # Órgãos federais com mais contratos
            codigos_orgao = [
                "26246",  # CGU
                "26443",  # Min. Educação
                "36000",  # Min. Saúde
                "53000",  # Min. Transportes
                "39000",  # Min. Infraestrutura
                "44000",  # Min. Meio Ambiente
                "25000",  # Min. Fazenda
                "30000",  # Min. Justiça
                "52000",  # Min. Defesa
                "20000",  # Presidência
            ]

        from horus.etl.transparencia import TransparenciaETL
        transp = TransparenciaETL(self.db)
        total = 0

        for cod in codigos_orgao:
            try:
                logger.info("Buscando contratos do órgão %s...", cod)
                raw = transp.extract_contratos(codigo_orgao=cod)
                if raw:
                    transformed = transp.transform({"contratos": raw})
                    if "contratos" in transformed and not transformed["contratos"].empty:
                        n = self.db.upsert_df("contratos", transformed["contratos"])
                        total += n
            except Exception as e:
                logger.warning("Erro contratos órgão %s: %s", cod, e)

        return total

    def enrich_sancoes(self) -> int:
        """Busca sanções CGU (CEIS, CNEP, CEAF, CEPIM)."""
        from horus.etl.cgu_sancoes import SancoesETL
        sancoes = SancoesETL(self.db)
        total = 0
        try:
            raw = sancoes.extract()
            df = sancoes.transform(raw)
            if not df.empty:
                total = sancoes.load(df)
        except Exception as e:
            logger.warning("Erro sanções: %s", e)
        return total

    def enrich_pncp(self) -> int:
        """Busca contratações do PNCP."""
        from horus.etl.pncp import PNCPETL
        pncp = PNCPETL(self.db)
        total = 0
        try:
            raw = pncp.extract()
            transformed = pncp.transform(raw)
            if isinstance(transformed, pd.DataFrame) and not transformed.empty:
                total = pncp.load(transformed)
            elif isinstance(transformed, dict):
                for t, df in transformed.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        total += self.db.upsert_df("contratos", df)
        except Exception as e:
            logger.warning("Erro PNCP: %s", e)
        return total

    def enrich_doacoes(self, anos: list[int] | None = None) -> int:
        """Busca doações de campanha do TSE.
        Retorna total de doações carregadas."""
        if anos is None:
            anos = [2022, 2020, 2018]

        from horus.etl.tse import TSEETL
        tse = TSEETL(self.db)
        total = 0

        for ano in anos:
            try:
                logger.info("TSE: buscando doações do ano %d...", ano)
                raw = tse.extract(ano=ano)
                if raw:
                    transformed = tse.transform(raw, ano=ano)
                    if isinstance(transformed, dict):
                        loaded = tse.load(transformed)
                        total += loaded
                        logger.info("TSE %d: %d registros carregados", ano, loaded)
            except Exception as e:
                logger.warning("Erro TSE doações %d: %s", ano, e)

        return total

    # ------------------------------------------------------------------
    # 3. Pipeline completo
    # ------------------------------------------------------------------

    def scan_all(self, skip_despesas: bool = False,
                 progress_callback=None) -> dict[str, Any]:
        """Executa o pipeline completo de varredura.
        
        Args:
            skip_despesas: Se True, pula coleta de despesas (demorado).
            progress_callback: Callable(etapa, detalhe) para progresso.
        
        Returns:
            Resumo da varredura.
        """
        scan_id = str(uuid.uuid4())[:8]
        inicio = datetime.now()
        resumo: dict[str, Any] = {"id": scan_id, "etapas": {}}

        def _log(etapa: str, detalhe: str = ""):
            logger.info("[SCAN %s] %s %s", scan_id, etapa, detalhe)
            if progress_callback:
                progress_callback(etapa, detalhe)

        # Registrar início
        with self.db.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO varreduras (id, inicio, status) VALUES (?, ?, ?)",
                (scan_id, inicio.isoformat(), "em_andamento"),
            )

        try:
            # Etapa 1: Descobrir políticos
            _log("DISCOVER", "Buscando deputados e senadores...")
            n_politicos = self.discover()
            resumo["etapas"]["politicos"] = n_politicos

            # Etapa 2: Emendas parlamentares
            _log("EMENDAS", "Coletando emendas parlamentares...")
            n_emendas = self.enrich_emendas()
            resumo["etapas"]["emendas"] = n_emendas

            # Etapa 3: Contratos federais
            _log("CONTRATOS", "Coletando contratos federais...")
            n_contratos = self.enrich_contratos()
            resumo["etapas"]["contratos"] = n_contratos

            # Etapa 4: Sanções CGU
            _log("SANCOES", "Verificando sanções CGU...")
            n_sancoes = self.enrich_sancoes()
            resumo["etapas"]["sancoes"] = n_sancoes

            # Etapa 5: PNCP
            _log("PNCP", "Coletando contratações PNCP...")
            n_pncp = self.enrich_pncp()
            resumo["etapas"]["pncp"] = n_pncp

            # Etapa 6: Doações de campanha TSE
            _log("TSE", "Coletando doações de campanha TSE...")
            n_doacoes = self.enrich_doacoes()
            resumo["etapas"]["doacoes"] = n_doacoes

            # Etapa 7: Despesas parlamentares (opcional, demorado)
            if not skip_despesas:
                _log("DESPESAS", "Coletando despesas parlamentares...")
                n_despesas = self.enrich_despesas()
                resumo["etapas"]["despesas"] = n_despesas

            # Etapa 8: Análise de anomalias
            _log("ANALISE", "Detectando anomalias e padrões...")
            from horus.anomaly_detector import AnomalyDetector
            detector = AnomalyDetector(self.db, self.config)
            insights = detector.detect_all()
            resumo["etapas"]["insights"] = len(insights)

            # Registrar conclusão
            fim = datetime.now()
            n_alertas = sum(1 for i in insights if i.severidade in ("CRITICO", "ALTO"))
            with self.db.connect() as conn:
                conn.execute("""
                    UPDATE varreduras
                    SET fim = ?, status = ?, total_politicos = ?,
                        total_insights = ?, total_alertas = ?, log_resumo = ?
                    WHERE id = ?
                """, (
                    fim.isoformat(), "concluido", n_politicos,
                    len(insights), n_alertas,
                    str(resumo["etapas"]), scan_id,
                ))

            resumo["status"] = "concluido"
            resumo["duracao_s"] = (fim - inicio).total_seconds()
            _log("CONCLUIDO", f"{len(insights)} insights, {n_alertas} alertas")

        except Exception as e:
            logger.error("Erro na varredura: %s", e)
            with self.db.connect() as conn:
                conn.execute(
                    "UPDATE varreduras SET fim = ?, status = ?, log_resumo = ? WHERE id = ?",
                    (datetime.now().isoformat(), "erro", str(e), scan_id),
                )
            resumo["status"] = "erro"
            resumo["erro"] = str(e)

        return resumo
