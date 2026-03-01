"""Daemon HORUS — Inicia o sistema completo (scheduler + dashboard).

Uso:
    python -m raiox.daemon           # Inicia tudo
    python -m raiox.daemon --no-web  # Só scheduler (sem dashboard)
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from raiox.config import Config
from raiox.database import DatabaseManager
from raiox.scheduler import HorusScheduler
from raiox.utils import get_logger

logger = get_logger(__name__)

BANNER = r"""
 ╔═══════════════════════════════════════════════════════╗
 ║                                                       ║
 ║    ⚡  H O R U S  — Sistema Autônomo de Rastreamento  ║
 ║                                                       ║
 ║    Monitoramento contínuo de dados públicos            ║
 ║    Análise de anomalias em tempo real                  ║
 ║    Dashboard atualizado automaticamente                ║
 ║                                                       ║
 ╚═══════════════════════════════════════════════════════╝
"""


def main():
    parser = argparse.ArgumentParser(description="HORUS Daemon — Sistema Autônomo")
    parser.add_argument("--no-web", action="store_true", help="Não inicia o dashboard web")
    parser.add_argument("--full-interval", type=float, default=6, help="Intervalo full scan (horas)")
    parser.add_argument("--quick-interval", type=float, default=1, help="Intervalo quick scan (horas)")
    parser.add_argument("--refresh-interval", type=float, default=15, help="Intervalo refresh (minutos)")
    parser.add_argument("--port", type=int, default=8501, help="Porta do dashboard")
    args = parser.parse_args()

    print(BANNER)
    
    config = Config()
    db = DatabaseManager(config)

    # --- Inicia o Scheduler ---
    scheduler = HorusScheduler(db, config)
    scheduler.start(
        run_initial_scan=True,
        full_interval_hours=args.full_interval,
        quick_interval_hours=args.quick_interval,
        refresh_interval_minutes=args.refresh_interval,
    )

    # --- Inicia o Dashboard (opcional) ---
    web_process = None
    if not args.no_web:
        web_py = str(Path(__file__).resolve().parent / "web.py")
        cmd = [
            sys.executable, "-m", "streamlit", "run", web_py,
            "--server.headless=true",
            f"--server.port={args.port}",
            "--server.runOnSave=false",
            "--browser.gatherUsageStats=false",
        ]
        logger.info("Iniciando dashboard em localhost:%d ...", args.port)
        web_process = subprocess.Popen(
            cmd,
            cwd=str(Path(__file__).resolve().parent.parent),
        )

    # --- Graceful shutdown ---
    def shutdown(signum=None, frame=None):
        logger.info("Encerrando HORUS...")
        scheduler.stop()
        if web_process:
            web_process.terminate()
            web_process.wait(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # --- Loop principal ---
    try:
        while True:
            status = scheduler.status
            task = status.get("current_task") or "idle"
            uptime = scheduler.get_uptime()
            scans = status.get("scan_count", 0)
            errors = status.get("error_count", 0)
            
            print(
                f"\r  HORUS ⚡ {uptime} | Scans: {scans} | "
                f"Erros: {errors} | Status: {task}    ",
                end="", flush=True,
            )
            time.sleep(5)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
