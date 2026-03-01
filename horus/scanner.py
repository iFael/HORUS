"""Scanner Automático de Políticos — Coleta e enriquecimento de dados.

Otimizado com ThreadPoolExecutor para máximo paralelismo I/O.
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pandas as pd

from horus.config import Config
from horus.database import DatabaseManager
from horus.utils import get_logger

logger = get_logger(__name__)

# Workers para paralelismo interno (dentro de cada etapa de enriquecimento)
_INNER_WORKERS = 4


class PoliticianScanner:
    """Pipeline automático: descobre políticos → enriquece dados → detecta anomalias."""

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self.config = config or db.config

    # ------------------------------------------------------------------
    # 1. Descoberta de políticos
    # ------------------------------------------------------------------

    def discover(self) -> int:
        """Busca todos os deputados e senadores em paralelo.
        Retorna total de políticos encontrados."""

        def _discover_camara() -> int:
            try:
                from horus.etl.camara import CamaraETL
                camara = CamaraETL(self.db)
                raw = camara.extract(legislatura=57)
                transformed = camara.transform(raw)
                if "politicos" in transformed:
                    camara.load(transformed)
                    n = len(transformed["politicos"])
                    logger.info("Câmara: %d deputados carregados", n)
                    return n
            except Exception as e:
                logger.error("Erro ao carregar deputados: %s", e)
            return 0

        def _discover_senado() -> int:
            try:
                from horus.etl.senado import SenadoETL
                senado = SenadoETL(self.db)
                raw = senado.extract()
                transformed = senado.transform(raw)
                if "politicos" in transformed:
                    senado.load(transformed)
                    n = len(transformed["politicos"])
                    logger.info("Senado: %d senadores carregados", n)
                    return n
            except Exception as e:
                logger.error("Erro ao carregar senadores: %s", e)
            return 0

        with ThreadPoolExecutor(max_workers=2) as exe:
            f_cam = exe.submit(_discover_camara)
            f_sen = exe.submit(_discover_senado)
            return f_cam.result() + f_sen.result()

    # ------------------------------------------------------------------
    # 2. Enriquecimento de dados
    # ------------------------------------------------------------------

    def enrich_despesas(self, anos: list[int] | None = None, max_deputados: int = 600) -> int:
        """Busca despesas parlamentares de deputados em paralelo.
        Retorna total de registros de despesas."""
        if anos is None:
            anos = [datetime.now().year - 1, datetime.now().year]

        from horus.etl.camara import CamaraETL
        camara = CamaraETL(self.db)
        politicos = self.db.buscar_politicos(cargo="Deputado Federal", limite=max_deputados)

        def _fetch_dep(pol: dict) -> int:
            dep_id = int(pol["id_externo"])
            count = 0
            for ano in anos:
                try:
                    raw = camara.extract_despesas(dep_id, ano)
                    if raw:
                        df = camara.transform_despesas(dep_id, raw)
                        if not df.empty:
                            camara.load(df)
                            count += len(df)
                except Exception as e:
                    logger.warning("Erro despesas %s/%d: %s", pol["nome"], ano, e)
            return count

        with ThreadPoolExecutor(max_workers=_INNER_WORKERS) as exe:
            total = sum(exe.map(_fetch_dep, politicos))

        logger.info("Total despesas coletadas: %d", total)
        return total

    def enrich_emendas(self, anos: list[int] | None = None) -> int:
        """Busca emendas parlamentares em paralelo por ano.
        Retorna total de emendas."""
        if anos is None:
            anos = [datetime.now().year - 1, datetime.now().year]

        from horus.etl.transparencia import TransparenciaETL
        transp = TransparenciaETL(self.db)

        def _fetch_ano(ano: int) -> int:
            try:
                logger.info("Buscando emendas do ano %d...", ano)
                raw_emendas = transp.extract_emendas(ano=ano)
                if raw_emendas:
                    transformed = transp.transform({"emendas": raw_emendas})
                    if "emendas" in transformed and not transformed["emendas"].empty:
                        n = self.db.upsert_df("emendas", transformed["emendas"])
                        logger.info("Ano %d: %d emendas", ano, n)
                        return n
            except Exception as e:
                logger.warning("Erro emendas %d: %s", ano, e)
            return 0

        with ThreadPoolExecutor(max_workers=len(anos)) as exe:
            total = sum(exe.map(_fetch_ano, anos))

        return total

    def enrich_contratos(self, codigos_orgao: list[str] | None = None) -> int:
        """Busca contratos em paralelo por órgão federal.
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

        def _fetch_orgao(cod: str) -> int:
            try:
                logger.info("Buscando contratos do órgão %s...", cod)
                raw = transp.extract_contratos(codigo_orgao=cod)
                if raw:
                    transformed = transp.transform({"contratos": raw})
                    if "contratos" in transformed and not transformed["contratos"].empty:
                        return self.db.upsert_df("contratos", transformed["contratos"])
            except Exception as e:
                logger.warning("Erro contratos órgão %s: %s", cod, e)
            return 0

        with ThreadPoolExecutor(max_workers=min(_INNER_WORKERS, len(codigos_orgao))) as exe:
            total = sum(exe.map(_fetch_orgao, codigos_orgao))

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
        """Busca doações de campanha do TSE em paralelo por ano.
        Retorna total de doações carregadas."""
        if anos is None:
            anos = [2022, 2020, 2018]

        from horus.etl.tse import TSEETL
        tse = TSEETL(self.db)

        def _fetch_ano(ano: int) -> int:
            try:
                logger.info("TSE: buscando doações do ano %d...", ano)
                raw = tse.extract(ano=ano)
                if raw:
                    transformed = tse.transform(raw, ano=ano)
                    if isinstance(transformed, dict):
                        loaded = tse.load(transformed)
                        logger.info("TSE %d: %d registros carregados", ano, loaded)
                        return loaded
            except Exception as e:
                logger.warning("Erro TSE doações %d: %s", ano, e)
            return 0

        with ThreadPoolExecutor(max_workers=len(anos)) as exe:
            total = sum(exe.map(_fetch_ano, anos))

        return total

    # ------------------------------------------------------------------
    # 2b. Fontes complementares (novos ETLs ativos)
    # ------------------------------------------------------------------

    def enrich_fontes_complementares(self) -> dict[str, int]:
        """Executa ETLs complementares (ANEEL, ANTT, SIAFI, INPE) em paralelo.
        Retorna dict {nome: registros_coletados}."""
        from horus.etl.registry import update_execution

        def _run_etl(nome: str, etl_cls, kwargs: dict) -> tuple[str, int]:
            try:
                etl = etl_cls(self.db)
                raw = etl.extract(**kwargs)
                if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
                    update_execution(nome, 0)
                    return nome, 0
                df = etl.transform(raw, **kwargs)
                count = etl.load(df, **kwargs) if not df.empty else 0
                update_execution(nome, count)
                logger.info("ETL %s: %d registros", nome, count)
                return nome, count
            except Exception as e:
                update_execution(nome, 0, str(e))
                logger.warning("ETL %s erro: %s", nome, e)
                return nome, 0

        # Importar apenas os que funcionam (testados)
        tasks = []
        try:
            from horus.etl.aneel import ANEELETL
            tasks.append(("aneel", ANEELETL, {}))
        except ImportError:
            pass
        try:
            from horus.etl.antt import ANTTETL
            tasks.append(("antt", ANTTETL, {}))
        except ImportError:
            pass
        try:
            from horus.etl.siafi import SIAFIETL
            tasks.append(("siafi", SIAFIETL, {"ano": datetime.now().year}))
        except ImportError:
            pass
        try:
            from horus.etl.inpe import INPEETL
            tasks.append(("inpe", INPEETL, {"max_features": 100}))
        except ImportError:
            pass

        result: dict[str, int] = {}
        if not tasks:
            return result

        with ThreadPoolExecutor(max_workers=min(4, len(tasks))) as exe:
            futures = {
                exe.submit(_run_etl, nome, cls, kw): nome
                for nome, cls, kw in tasks
            }
            for future in as_completed(futures):
                nome, count = future.result()
                result[nome] = count

        logger.info("Fontes complementares: %s", result)
        return result

    # ------------------------------------------------------------------
    # 3. Pipeline completo
    # ------------------------------------------------------------------

    def scan_all(self, skip_despesas: bool = False,
                 progress_callback=None) -> dict[str, Any]:
        """Executa o pipeline completo de varredura com máximo paralelismo.

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
            # Etapa 1: Descobrir políticos (deve ser primeiro)
            _log("DISCOVER", "Buscando deputados e senadores...")
            n_politicos = self.discover()
            resumo["etapas"]["politicos"] = n_politicos

            # Etapa 2: Enriquecimento em paralelo (múltiplas fontes simultâneas)
            _log("ENRIQUECIMENTO", "Coletando dados de múltiplas fontes em paralelo...")

            enrich_tasks: dict[str, Any] = {
                "emendas": self.enrich_emendas,
                "contratos": self.enrich_contratos,
                "sancoes": self.enrich_sancoes,
                "pncp": self.enrich_pncp,
                "doacoes": self.enrich_doacoes,
            }

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(fn): name
                    for name, fn in enrich_tasks.items()
                }
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result()
                        resumo["etapas"][name] = result
                        _log(name.upper(), f"{result} registros")
                    except Exception as e:
                        logger.error("Erro em %s: %s", name, e)
                        resumo["etapas"][name] = 0

            # Etapa 3: Despesas parlamentares (opcional, demorado)
            if not skip_despesas:
                _log("DESPESAS", "Coletando despesas parlamentares...")
                n_despesas = self.enrich_despesas()
                resumo["etapas"]["despesas"] = n_despesas

            # Etapa 4: Fontes complementares (ANEEL, ANTT, SIAFI, INPE)
            _log("FONTES_EXTRAS", "Coletando fontes complementares...")
            try:
                extras = self.enrich_fontes_complementares()
                resumo["etapas"]["fontes_extras"] = extras
                _log("FONTES_EXTRAS", f"{sum(extras.values())} registros de {len(extras)} fontes")
            except Exception as e:
                logger.warning("Erro fontes extras: %s", e)
                resumo["etapas"]["fontes_extras"] = {}

            # Etapa 5: Análise de anomalias (precisa de todos os dados)
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
