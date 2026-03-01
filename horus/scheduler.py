"""Scheduler Autônomo — Mantém o sistema HORUS vivo e atualizado."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from horus.config import Config
from horus.database import DatabaseManager
from horus.utils import get_logger
from horus.auditor import InternalAuditor

logger = get_logger(__name__)


class HorusScheduler:
    """Scheduler que roda varreduras automaticamente em background.
    
    Ciclos:
        - FULL SCAN:   a cada 6h  (descobre políticos + coleta tudo + anomalias)
        - QUICK SCAN:  a cada 1h  (emendas + contratos + anomalias)
        - REFRESH:     a cada 15min (re-analisa anomalias com dados existentes)
    """

    _instance: HorusScheduler | None = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton — só um scheduler por processo."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db: DatabaseManager | None = None,
                 config: Config | None = None) -> None:
        if self._initialized:
            return
        
        self._config = config or Config()
        self._db = db or DatabaseManager(self._config)
        self._scheduler = BackgroundScheduler(
            timezone="America/Sao_Paulo",
            job_defaults={"coalesce": True, "max_instances": 1},
        )
        self._status: dict[str, Any] = {
            "running": False,
            "last_full_scan": None,
            "last_quick_scan": None,
            "last_refresh": None,
            "next_full_scan": None,
            "next_quick_scan": None,
            "next_refresh": None,
            "current_task": None,
            "error_count": 0,
            "scan_count": 0,
            "start_time": None,
        }
        self._listeners: list[Callable] = []
        self._auditor: InternalAuditor | None = None
        self._initialized = True

    @property
    def status(self) -> dict[str, Any]:
        """Status atual do scheduler (thread-safe read)."""
        # Atualiza próximos jobs
        for job_id, key in [("full_scan", "next_full_scan"),
                            ("quick_scan", "next_quick_scan"),
                            ("refresh_insights", "next_refresh")]:
            job = self._scheduler.get_job(job_id)
            if job and job.next_run_time:
                self._status[key] = job.next_run_time.isoformat()
        return dict(self._status)

    @property
    def is_running(self) -> bool:
        return self._status["running"]

    def add_listener(self, callback: Callable[[str, dict], None]) -> None:
        """Registra listener para eventos do scheduler."""
        self._listeners.append(callback)

    def _notify(self, event: str, data: dict | None = None) -> None:
        for cb in self._listeners:
            try:
                cb(event, data or {})
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def _job_full_scan(self) -> None:
        """Varredura completa: descobre + coleta + analisa."""
        self._status["current_task"] = "FULL SCAN"
        self._notify("scan_start", {"type": "full"})
        logger.info("═══ FULL SCAN INICIANDO ═══")

        try:
            from horus.scanner import PoliticianScanner
            scanner = PoliticianScanner(self._db, self._config)
            result = scanner.scan_all(skip_despesas=True)

            self._status["last_full_scan"] = datetime.now().isoformat()
            self._status["scan_count"] += 1
            self._notify("scan_complete", {"type": "full", "result": result})
            logger.info("═══ FULL SCAN CONCLUÍDO: %s ═══", result.get("etapas", {}))

        except Exception as e:
            self._status["error_count"] += 1
            logger.error("FULL SCAN ERRO: %s", e)
            self._notify("scan_error", {"type": "full", "error": str(e)})
        finally:
            self._status["current_task"] = None

    def _job_quick_scan(self) -> None:
        """Varredura rápida: emendas + contratos + anomalias (sem re-descoberta)."""
        self._status["current_task"] = "QUICK SCAN"
        self._notify("scan_start", {"type": "quick"})
        logger.info("── QUICK SCAN INICIANDO ──")

        scan_id = f"quick_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        inicio = datetime.now()
        try:
            with self._db.connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO varreduras (id, inicio, status) VALUES (?, ?, ?)",
                    (scan_id, inicio.isoformat(), "em_andamento"),
                )
        except Exception:
            pass

        try:
            from horus.scanner import PoliticianScanner
            scanner = PoliticianScanner(self._db, self._config)

            # Só coleta dados novos + re-analisa
            n_emendas = scanner.enrich_emendas()
            n_contratos = scanner.enrich_contratos()
            n_sancoes = scanner.enrich_sancoes()

            # Re-detectar anomalias
            from horus.anomaly_detector import AnomalyDetector
            detector = AnomalyDetector(self._db, self._config)
            insights = detector.detect_all()

            self._status["last_quick_scan"] = datetime.now().isoformat()
            self._status["scan_count"] += 1
            result = {
                "emendas": n_emendas,
                "contratos": n_contratos,
                "sancoes": n_sancoes,
                "insights": len(insights),
            }

            # Registrar conclusão na tabela de varreduras
            try:
                n_pol = len(self._db.buscar_politicos(limite=10000))
                with self._db.connect() as conn:
                    conn.execute(
                        "UPDATE varreduras SET fim = ?, status = ?, total_politicos = ?, "
                        "total_insights = ?, log_resumo = ? WHERE id = ?",
                        (datetime.now().isoformat(), "concluido", n_pol,
                         len(insights), str(result), scan_id),
                    )
            except Exception:
                pass

            self._notify("scan_complete", {"type": "quick", "result": result})
            logger.info("── QUICK SCAN CONCLUÍDO: %s ──", result)

        except Exception as e:
            self._status["error_count"] += 1
            logger.error("QUICK SCAN ERRO: %s", e)
            try:
                with self._db.connect() as conn:
                    conn.execute(
                        "UPDATE varreduras SET fim = ?, status = ?, log_resumo = ? WHERE id = ?",
                        (datetime.now().isoformat(), "erro", str(e), scan_id),
                    )
            except Exception:
                pass
            self._notify("scan_error", {"type": "quick", "error": str(e)})
        finally:
            self._status["current_task"] = None

    def _job_refresh(self) -> None:
        """Refresh rápido: re-analisa anomalias com dados existentes."""
        self._status["current_task"] = "REFRESH"
        logger.debug("- Refresh insights -")

        scan_id = f"refresh_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        inicio = datetime.now()
        try:
            with self._db.connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO varreduras (id, inicio, status) VALUES (?, ?, ?)",
                    (scan_id, inicio.isoformat(), "em_andamento"),
                )
        except Exception:
            pass

        try:
            from horus.anomaly_detector import AnomalyDetector
            detector = AnomalyDetector(self._db, self._config)
            insights = detector.detect_all()

            self._status["last_refresh"] = datetime.now().isoformat()

            # Registrar conclus\u00e3o
            try:
                n_pol = len(self._db.buscar_politicos(limite=10000))
                with self._db.connect() as conn:
                    conn.execute(
                        "UPDATE varreduras SET fim = ?, status = ?, total_politicos = ?, "
                        "total_insights = ?, log_resumo = ? WHERE id = ?",
                        (datetime.now().isoformat(), "concluido", n_pol,
                         len(insights), "refresh", scan_id),
                    )
            except Exception:
                pass

            self._notify("refresh_complete", {"insights": len(insights)})

        except Exception as e:
            self._status["error_count"] += 1
            logger.warning("REFRESH ERRO: %s", e)
            try:
                with self._db.connect() as conn:
                    conn.execute(
                        "UPDATE varreduras SET fim = ?, status = ?, log_resumo = ? WHERE id = ?",
                        (datetime.now().isoformat(), "erro", str(e), scan_id),
                    )
            except Exception:
                pass
        finally:
            self._status["current_task"] = None

    # ------------------------------------------------------------------
    # Controle
    # ------------------------------------------------------------------

    def start(self, run_initial_scan: bool = True,
              full_interval_hours: float = 6,
              quick_interval_hours: float = 1,
              refresh_interval_minutes: float = 15) -> None:
        """Inicia o scheduler com os intervalos configurados.
        
        Args:
            run_initial_scan: Se True, roda um full scan imediatamente na primeira vez.
            full_interval_hours: Intervalo entre full scans (padrão 6h).
            quick_interval_hours: Intervalo entre quick scans (padrão 1h).
            refresh_interval_minutes: Intervalo entre refreshes (padrão 15min).
        """
        if self._status["running"]:
            logger.info("Scheduler já está rodando")
            return

        # Adiciona os jobs
        self._scheduler.add_job(
            self._job_full_scan,
            trigger=IntervalTrigger(hours=full_interval_hours),
            id="full_scan",
            name="Full Scan (Descoberta + Coleta + Análise)",
            replace_existing=True,
        )

        self._scheduler.add_job(
            self._job_quick_scan,
            trigger=IntervalTrigger(hours=quick_interval_hours),
            id="quick_scan",
            name="Quick Scan (Coleta + Análise)",
            replace_existing=True,
        )

        self._scheduler.add_job(
            self._job_refresh,
            trigger=IntervalTrigger(minutes=refresh_interval_minutes),
            id="refresh_insights",
            name="Refresh Insights",
            replace_existing=True,
        )

        # Event listener para logs
        def _on_event(event):
            if event.exception:
                logger.error("Job falhou: %s", event.exception)
        self._scheduler.add_listener(_on_event, EVENT_JOB_ERROR)

        self._scheduler.start()
        self._status["running"] = True
        self._status["start_time"] = datetime.now().isoformat()

        # Auditor autônomo — thread própria, independente do APScheduler
        self._auditor = InternalAuditor(self._db, self._config)

        def _audit_status_cb(result):
            self._status["last_audit"] = datetime.now().isoformat()
            self._status["audit_issues_fixed"] = self._status.get("audit_issues_fixed", 0) + result.issues_fixed
            self._notify("audit_complete", {
                "issues_found": result.issues_found,
                "issues_fixed": result.issues_fixed,
                "checks": result.checks_run,
            })

        self._auditor.set_status_callback(_audit_status_cb)
        self._auditor.start(interval_minutes=10)

        logger.info(
            "╔══════════════════════════════════════════╗\n"
            "║   HORUS SCHEDULER ATIVO                  ║\n"
            "║   Full Scan:  a cada %sh                ║\n"
            "║   Quick Scan: a cada %sh                ║\n"
            "║   Refresh:    a cada %smin              ║\n"
            "║   Auditor:    AUTÔNOMO (ciclo 10min)    ║\n"
            "╚══════════════════════════════════════════╝",
            full_interval_hours, quick_interval_hours, refresh_interval_minutes,
        )

        # Roda scan inicial em thread separada se banco está vazio
        if run_initial_scan:
            threading.Thread(
                target=self._initial_scan,
                daemon=True,
                name="horus-initial-scan",
            ).start()

    def _initial_scan(self) -> None:
        """Executa scan inicial se o banco está vazio ou desatualizado."""
        time.sleep(3)  # Espera o dashboard renderizar primeiro

        # Limpa varreduras travadas (em_andamento > 1h = processo morreu)
        try:
            with self._db.connect() as conn:
                conn.execute(
                    "UPDATE varreduras SET status = 'interrompido', fim = ? "
                    "WHERE status = 'em_andamento' AND inicio < ?",
                    (datetime.now().isoformat(),
                     (datetime.now() - timedelta(hours=1)).isoformat()),
                )
        except Exception:
            pass

        try:
            stats = self._db.get_dashboard_stats()
            n_politicos = stats.get("politicos", 0)
            ultima = stats.get("ultima_varredura")

            needs_scan = False
            
            if n_politicos == 0:
                logger.info("Banco vazio — executando scan inicial completo...")
                needs_scan = True
            elif ultima:
                last_time = datetime.fromisoformat(ultima.get("inicio", "2000-01-01"))
                if datetime.now() - last_time > timedelta(hours=6):
                    logger.info("Última varredura > 6h — executando scan completo...")
                    needs_scan = True
            
            if needs_scan:
                self._job_full_scan()
            else:
                logger.info("Dados atualizados — próximo scan agendado normalmente")
                # Mesmo assim faz um refresh rápido
                self._job_refresh()

        except Exception as e:
            logger.error("Erro no scan inicial: %s", e)

    def stop(self) -> None:
        """Para o scheduler e o auditor."""
        if self._status["running"]:
            self._scheduler.shutdown(wait=False)
            if self._auditor is not None:
                self._auditor.stop()
            self._status["running"] = False
            logger.info("HORUS Scheduler parado")

    def force_scan(self, scan_type: str = "full") -> None:
        """Força uma varredura imediata."""
        if scan_type == "full":
            threading.Thread(target=self._job_full_scan, daemon=True).start()
        elif scan_type == "quick":
            threading.Thread(target=self._job_quick_scan, daemon=True).start()
        elif scan_type == "refresh":
            threading.Thread(target=self._job_refresh, daemon=True).start()

    def get_uptime(self) -> str:
        """Retorna uptime formatado."""
        if not self._status["start_time"]:
            return "Offline"
        start = datetime.fromisoformat(self._status["start_time"])
        delta = datetime.now() - start
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 24:
            days = hours // 24
            hours = hours % 24
            return f"{days}d {hours}h {minutes}m"
        return f"{hours}h {minutes}m {seconds}s"
