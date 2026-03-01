"""Auditor Interno Autônomo — Verificação contínua de integridade.

Roda em segundo plano, de forma intermitente, e garante que:
- Dados no banco não contêm campos inválidos, nulos críticos ou lixo
- CPFs/CNPJs armazenados são consistentes (11 ou 14 dígitos numéricos)
- Insights têm evidências válidas (os dados referenciados existem)
- Registros duplicados são eliminados
- Valores financeiros são positivos e realistas (sem overflow/negativos)
- Cross-references entre tabelas são íntegras (FK virtual)
- Varreduras travadas são detectadas e limpas
"""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from raiox.database import DatabaseManager
from raiox.utils import get_logger
from raiox.config import Config

logger = get_logger(__name__)

_RE_CPF = re.compile(r"^\d{11}$")
_RE_CNPJ = re.compile(r"^\d{14}$")
_RE_CPF_CNPJ = re.compile(r"^\d{11}(\d{3})?$")

# Valor máximo plausível para um contrato/emenda/despesa (R$100 bilhões)
_VALOR_MAX_PLAUSIVEL = 100_000_000_000.0


@dataclass
class AuditResult:
    """Resultado de um ciclo de auditoria."""

    timestamp: str = ""
    checks_run: int = 0
    issues_found: int = 0
    issues_fixed: int = 0
    details: list[str] = field(default_factory=list)
    duration_ms: int = 0

    def log_issue(self, msg: str, fixed: bool = False) -> None:
        self.issues_found += 1
        if fixed:
            self.issues_fixed += 1
        tag = "CORRIGIDO" if fixed else "DETECTADO"
        entry = f"[{tag}] {msg}"
        self.details.append(entry)
        logger.warning("AUDITORIA: %s", entry)


class InternalAuditor:
    """Auditor autônomo que verifica integridade dos dados continuamente.

    Roda integrado ao scheduler em ciclos de 10 minutos, executando
    verificações leves e autocorretivas.
    """

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self._config = config
        self._running = False
        self._thread: threading.Thread | None = None
        self._last_result: AuditResult | None = None
        self._history: list[AuditResult] = []
        self._cycle_count = 0
        self._status_callback: Callable | None = None

    @property
    def last_result(self) -> AuditResult | None:
        return self._last_result

    @property
    def history(self) -> list[AuditResult]:
        return list(self._history[-50:])

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------

    def start(self, interval_minutes: float = 10) -> None:
        """Inicia auditoria contínua em background."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop,
            args=(interval_minutes,),
            daemon=True,
            name="orus-auditor",
        )
        self._thread.start()
        logger.info(
            "🛡 Auditor interno iniciado (ciclo a cada %.0f min)", interval_minutes
        )

    def stop(self) -> None:
        self._running = False
        logger.info("Auditor interno parado")

    def set_status_callback(self, cb: Callable) -> None:
        """Define callback para atualizar status do scheduler."""
        self._status_callback = cb

    def _loop(self, interval_minutes: float) -> None:
        """Loop infinito de auditoria — 100% autônomo."""
        time.sleep(30)  # Espera sistema estabilizar antes do primeiro ciclo
        while self._running:
            try:
                result = self.run_audit_cycle()
                # Atualiza status do scheduler se callback configurado
                if self._status_callback:
                    self._status_callback(result)
                # Se corrigiu problemas, regenera insights automaticamente
                if result.issues_fixed > 0:
                    self._regenerar_insights()
            except Exception as e:
                logger.error("Erro no ciclo de auditoria: %s", e)
            time.sleep(interval_minutes * 60)

    # ------------------------------------------------------------------
    # Ciclo de auditoria
    # ------------------------------------------------------------------

    def run_audit_cycle(self) -> AuditResult:
        """Executa um ciclo completo de auditoria."""
        t0 = time.monotonic()
        result = AuditResult(timestamp=datetime.now().isoformat())
        self._cycle_count += 1

        checks = [
            self._check_cpf_cnpj_invalidos,
            self._check_valores_financeiros,
            self._check_insights_orfaos,
            self._check_duplicatas_contratos,
            self._check_duplicatas_sancoes,
            self._check_varreduras_travadas,
            self._check_doacoes_invalidas,
            self._check_emendas_invalidas,
            self._check_despesas_invalidas,
            self._check_integridade_referencias,
        ]

        for check_fn in checks:
            try:
                check_fn(result)
                result.checks_run += 1
            except Exception as e:
                logger.warning("Auditoria - check '%s' falhou: %s", check_fn.__name__, e)

        elapsed = int((time.monotonic() - t0) * 1000)
        result.duration_ms = elapsed

        self._last_result = result
        self._history.append(result)
        if len(self._history) > 100:
            self._history = self._history[-50:]

        level = "WARNING" if result.issues_found > 0 else "INFO"
        logger.log(
            30 if level == "WARNING" else 20,
            "Auditoria #%d concluída em %dms: %d checks, %d problemas, %d corrigidos",
            self._cycle_count,
            elapsed,
            result.checks_run,
            result.issues_found,
            result.issues_fixed,
        )

        return result

    def _regenerar_insights(self) -> None:
        """Regenera insights automaticamente após correções."""
        try:
            from raiox.anomaly_detector import AnomalyDetector
            detector = AnomalyDetector(self.db, self._config or Config())
            insights = detector.detect_all()
            logger.info(
                "🔄 Insights regenerados automaticamente pelo auditor: %d",
                len(insights),
            )
        except Exception as e:
            logger.warning("Falha ao regenerar insights pós-auditoria: %s", e)

    # ==================================================================
    # CHECKS INDIVIDUAIS
    # ==================================================================

    def _check_cpf_cnpj_invalidos(self, r: AuditResult) -> None:
        """Verifica se há CPFs/CNPJs inválidos (não numéricos, tamanho errado)."""
        # Contratos com CNPJ de fornecedor inválido
        bad_contratos = self.db.query("""
            SELECT COUNT(*) AS cnt FROM contratos
            WHERE fornecedor_cnpj != ''
              AND (
                  length(fornecedor_cnpj) < 11
                  OR fornecedor_cnpj GLOB '*[^0-9]*'
              )
        """)
        n = bad_contratos[0]["cnt"] if bad_contratos else 0
        if n > 0:
            # Limpar registros com CNPJ lixo — zeramos o campo para que não
            # sejam usados em cruzamentos
            with self.db.connect() as conn:
                conn.execute("""
                    UPDATE contratos SET fornecedor_cnpj = ''
                    WHERE fornecedor_cnpj != ''
                      AND (
                          length(fornecedor_cnpj) < 11
                          OR fornecedor_cnpj GLOB '*[^0-9]*'
                      )
                """)
            r.log_issue(
                f"{n} contrato(s) com CNPJ de fornecedor inválido → campo zerado",
                fixed=True,
            )

        # Doações com CPF/CNPJ inválido
        bad_doacoes = self.db.query("""
            SELECT COUNT(*) AS cnt FROM doacoes
            WHERE cpf_cnpj_doador != ''
              AND (
                  length(cpf_cnpj_doador) < 11
                  OR cpf_cnpj_doador GLOB '*[^0-9]*'
              )
        """)
        n = bad_doacoes[0]["cnt"] if bad_doacoes else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute("""
                    UPDATE doacoes SET cpf_cnpj_doador = ''
                    WHERE cpf_cnpj_doador != ''
                      AND (
                          length(cpf_cnpj_doador) < 11
                          OR cpf_cnpj_doador GLOB '*[^0-9]*'
                      )
                """)
            r.log_issue(
                f"{n} doação(ões) com CPF/CNPJ de doador inválido → campo zerado",
                fixed=True,
            )

        # Sanções com CPF/CNPJ inválido
        bad_sancoes = self.db.query("""
            SELECT COUNT(*) AS cnt FROM sancoes
            WHERE cpf_cnpj != ''
              AND (
                  length(cpf_cnpj) < 11
                  OR cpf_cnpj GLOB '*[^0-9]*'
              )
        """)
        n = bad_sancoes[0]["cnt"] if bad_sancoes else 0
        if n > 0:
            # Deletar em vez de zerar — zerar pode violar UNIQUE constraint
            with self.db.connect() as conn:
                conn.execute("""
                    DELETE FROM sancoes
                    WHERE cpf_cnpj != ''
                      AND (
                          length(cpf_cnpj) < 11
                          OR cpf_cnpj GLOB '*[^0-9]*'
                      )
                """)
            r.log_issue(
                f"{n} sanção(ões) com CPF/CNPJ inválido → removida(s)",
                fixed=True,
            )

    def _check_valores_financeiros(self, r: AuditResult) -> None:
        """Verifica valores negativos ou absurdamente altos."""
        # Contratos com valor negativo
        neg = self.db.query(
            "SELECT COUNT(*) AS cnt FROM contratos WHERE valor < 0"
        )
        n = neg[0]["cnt"] if neg else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "UPDATE contratos SET valor = ABS(valor) WHERE valor < 0"
                )
            r.log_issue(f"{n} contrato(s) com valor negativo → convertido para positivo", fixed=True)

        # Contratos com valor absurdo (>R$100bi)
        absurdo = self.db.query(
            "SELECT COUNT(*) AS cnt FROM contratos WHERE valor > ?",
            (_VALOR_MAX_PLAUSIVEL,),
        )
        n = absurdo[0]["cnt"] if absurdo else 0
        if n > 0:
            r.log_issue(
                f"{n} contrato(s) com valor > R$100bi detectado(s) — possível dado corrompido"
            )

        # Emendas com valor negativo
        neg_em = self.db.query(
            "SELECT COUNT(*) AS cnt FROM emendas WHERE valor_empenhado < 0"
        )
        n = neg_em[0]["cnt"] if neg_em else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "UPDATE emendas SET valor_empenhado = ABS(valor_empenhado) WHERE valor_empenhado < 0"
                )
            r.log_issue(f"{n} emenda(s) com valor negativo → convertido para positivo", fixed=True)

        # Despesas com valor negativo
        neg_dp = self.db.query(
            "SELECT COUNT(*) AS cnt FROM despesas_parlamentares WHERE valor < 0"
        )
        n = neg_dp[0]["cnt"] if neg_dp else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "UPDATE despesas_parlamentares SET valor = ABS(valor) WHERE valor < 0"
                )
            r.log_issue(f"{n} despesa(s) com valor negativo → convertido para positivo", fixed=True)

    def _check_insights_orfaos(self, r: AuditResult) -> None:
        """Remove insights cujos dados de suporte não existem mais."""
        # Insight tipo fornecedor_sancionado referencia CNPJ que não está mais em sancoes
        orfaos = self.db.query("""
            SELECT i.id, json_extract(i.dados_json, '$.cpf_cnpj') as cpf_cnpj
            FROM insights i
            WHERE i.tipo = 'fornecedor_sancionado'
              AND json_extract(i.dados_json, '$.cpf_cnpj') IS NOT NULL
              AND json_extract(i.dados_json, '$.cpf_cnpj') NOT IN (
                  SELECT cpf_cnpj FROM sancoes WHERE cpf_cnpj != ''
              )
        """)
        if orfaos:
            ids = [o["id"] for o in orfaos]
            placeholders = ",".join("?" for _ in ids)
            with self.db.connect() as conn:
                conn.execute(
                    f"DELETE FROM insights WHERE id IN ({placeholders})", ids
                )
            r.log_issue(
                f"{len(orfaos)} insight(s) de fornecedor_sancionado sem sanção correspondente → removido(s)",
                fixed=True,
            )

        # Insight com score fora do range [0, 100]
        bad_score = self.db.query(
            "SELECT COUNT(*) AS cnt FROM insights WHERE score < 0 OR score > 100"
        )
        n = bad_score[0]["cnt"] if bad_score else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "UPDATE insights SET score = MIN(100, MAX(0, score)) WHERE score < 0 OR score > 100"
                )
            r.log_issue(
                f"{n} insight(s) com score fora de [0..100] → normalizado", fixed=True
            )

    def _check_duplicatas_contratos(self, r: AuditResult) -> None:
        """Detecta contratos duplicados (mesmo número, órgão, fonte)."""
        dups = self.db.query("""
            SELECT numero, orgao_cnpj, fonte, COUNT(*) AS cnt
            FROM contratos
            WHERE numero IS NOT NULL AND numero != ''
            GROUP BY numero, orgao_cnpj, fonte
            HAVING cnt > 1
        """)
        total_dups = sum(d["cnt"] - 1 for d in dups) if dups else 0
        if total_dups > 0:
            # Remove duplicatas mantendo o registro com menor ID (mais antigo)
            with self.db.connect() as conn:
                conn.execute("""
                    DELETE FROM contratos
                    WHERE id NOT IN (
                        SELECT MIN(id) FROM contratos
                        WHERE numero IS NOT NULL AND numero != ''
                        GROUP BY numero, orgao_cnpj, fonte
                    )
                    AND numero IS NOT NULL AND numero != ''
                """)
            r.log_issue(
                f"{total_dups} contrato(s) duplicado(s) → removido(s)", fixed=True
            )

    def _check_duplicatas_sancoes(self, r: AuditResult) -> None:
        """Detecta sanções duplicadas."""
        dups = self.db.query("""
            SELECT tipo, cpf_cnpj, data_inicio, COUNT(*) AS cnt
            FROM sancoes
            WHERE cpf_cnpj != ''
            GROUP BY tipo, cpf_cnpj, data_inicio
            HAVING cnt > 1
        """)
        total_dups = sum(d["cnt"] - 1 for d in dups) if dups else 0
        if total_dups > 0:
            with self.db.connect() as conn:
                conn.execute("""
                    DELETE FROM sancoes
                    WHERE id NOT IN (
                        SELECT MIN(id) FROM sancoes
                        WHERE cpf_cnpj != ''
                        GROUP BY tipo, cpf_cnpj, data_inicio
                    )
                    AND cpf_cnpj != ''
                """)
            r.log_issue(
                f"{total_dups} sanção(ões) duplicada(s) → removida(s)", fixed=True
            )

    def _check_varreduras_travadas(self, r: AuditResult) -> None:
        """Marca varreduras 'em_andamento' há mais de 1h como interrompidas."""
        cutoff = (datetime.now() - timedelta(hours=1)).isoformat()
        stale = self.db.query(
            "SELECT COUNT(*) AS cnt FROM varreduras WHERE status = 'em_andamento' AND inicio < ?",
            (cutoff,),
        )
        n = stale[0]["cnt"] if stale else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "UPDATE varreduras SET status = 'interrompido', fim = ? "
                    "WHERE status = 'em_andamento' AND inicio < ?",
                    (datetime.now().isoformat(), cutoff),
                )
            r.log_issue(
                f"{n} varredura(s) travada(s) → marcada(s) como interrompida(s)",
                fixed=True,
            )

    def _check_doacoes_invalidas(self, r: AuditResult) -> None:
        """Verifica doações com dados faltantes ou inconsistentes."""
        # Doações com valor zero ou nulo
        bad = self.db.query(
            "SELECT COUNT(*) AS cnt FROM doacoes WHERE valor IS NULL OR valor <= 0"
        )
        n = bad[0]["cnt"] if bad else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "DELETE FROM doacoes WHERE valor IS NULL OR valor <= 0"
                )
            r.log_issue(
                f"{n} doação(ões) com valor nulo/zero → removida(s)", fixed=True
            )

        # Doações sem nome de candidato nem CPF
        bad2 = self.db.query(
            "SELECT COUNT(*) AS cnt FROM doacoes WHERE (nome_candidato IS NULL OR nome_candidato = '') AND (cpf_candidato IS NULL OR cpf_candidato = '')"
        )
        n = bad2[0]["cnt"] if bad2 else 0
        if n > 0:
            r.log_issue(
                f"{n} doação(ões) sem candidato identificado — dado incompleto"
            )

    def _check_emendas_invalidas(self, r: AuditResult) -> None:
        """Verifica emendas sem autor ou com valor inválido."""
        bad = self.db.query(
            "SELECT COUNT(*) AS cnt FROM emendas WHERE autor IS NULL OR autor = ''"
        )
        n = bad[0]["cnt"] if bad else 0
        if n > 0:
            r.log_issue(f"{n} emenda(s) sem autor identificado")

        # Emendas com ano impossível
        bad_ano = self.db.query(
            "SELECT COUNT(*) AS cnt FROM emendas WHERE ano IS NOT NULL AND (ano < 2000 OR ano > 2030)"
        )
        n = bad_ano[0]["cnt"] if bad_ano else 0
        if n > 0:
            r.log_issue(f"{n} emenda(s) com ano fora do intervalo 2000-2030")

    def _check_despesas_invalidas(self, r: AuditResult) -> None:
        """Verifica despesas parlamentares inconsistentes."""
        # Despesas sem politico_id
        bad = self.db.query(
            "SELECT COUNT(*) AS cnt FROM despesas_parlamentares WHERE politico_id IS NULL OR politico_id = ''"
        )
        n = bad[0]["cnt"] if bad else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute(
                    "DELETE FROM despesas_parlamentares WHERE politico_id IS NULL OR politico_id = ''"
                )
            r.log_issue(
                f"{n} despesa(s) sem politico_id → removida(s)", fixed=True
            )

        # Despesas com fornecedor_cnpj inválido
        bad2 = self.db.query("""
            SELECT COUNT(*) AS cnt FROM despesas_parlamentares
            WHERE fornecedor_cnpj != ''
              AND (length(fornecedor_cnpj) < 11 OR fornecedor_cnpj GLOB '*[^0-9]*')
        """)
        n = bad2[0]["cnt"] if bad2 else 0
        if n > 0:
            with self.db.connect() as conn:
                conn.execute("""
                    UPDATE despesas_parlamentares SET fornecedor_cnpj = ''
                    WHERE fornecedor_cnpj != ''
                      AND (length(fornecedor_cnpj) < 11 OR fornecedor_cnpj GLOB '*[^0-9]*')
                """)
            r.log_issue(
                f"{n} despesa(s) com CNPJ de fornecedor inválido → campo zerado",
                fixed=True,
            )

    def _check_integridade_referencias(self, r: AuditResult) -> None:
        """Verifica integridade de referências cruzadas entre tabelas."""
        # Despesas referenciando politico_id que não existe na tabela politicos
        orfaos = self.db.query("""
            SELECT COUNT(DISTINCT dp.politico_id) AS cnt
            FROM despesas_parlamentares dp
            LEFT JOIN politicos p ON dp.politico_id = p.id
            WHERE p.id IS NULL AND dp.politico_id != ''
        """)
        n = orfaos[0]["cnt"] if orfaos else 0
        if n > 0:
            r.log_issue(
                f"{n} politico_id(s) em despesas_parlamentares sem correspondência em politicos"
            )

        # Insights referenciando politico_id inexistente
        orfaos_ins = self.db.query("""
            SELECT COUNT(DISTINCT i.politico_id) AS cnt
            FROM insights i
            LEFT JOIN politicos p ON i.politico_id = p.id
            WHERE i.politico_id IS NOT NULL AND i.politico_id != '' AND p.id IS NULL
        """)
        n = orfaos_ins[0]["cnt"] if orfaos_ins else 0
        if n > 0:
            r.log_issue(
                f"{n} insight(s) referenciando politico_id inexistente"
            )

        # Sócios referenciando CNPJ que não existe em empresas
        # (Nota: nem todo CNPJ tem cadastro — é informativo, não correção)
        orfaos_soc = self.db.query("""
            SELECT COUNT(DISTINCT s.cnpj) AS cnt
            FROM socios s
            LEFT JOIN empresas e ON s.cnpj = e.cnpj
            WHERE e.cnpj IS NULL AND s.cnpj != ''
        """)
        n = orfaos_soc[0]["cnt"] if orfaos_soc else 0
        if n > 0:
            r.log_issue(
                f"{n} CNPJ(s) em sócios sem cadastro correspondente em empresas (informativo)"
            )
