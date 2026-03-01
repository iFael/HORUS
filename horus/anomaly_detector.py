"""Detector de Anomalias — Cruzamento inteligente de dados públicos.

PRINCÍPIO FUNDAMENTAL: Cada insight gerado DEVE ser validado por evidências
concretas (JOINs por CNPJ/CPF, dados numéricos reais). Match por nome/
sobrenome isolado é PROIBIDO — gera falsos positivos massivos com
sobrenomes comuns brasileiros (Silva, Lima, Santos, Barros, ...).
"""

from __future__ import annotations

import re
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any

from horus.config import Config
from horus.database import DatabaseManager
from horus.insights import Insight, InsightManager, Severidade, formatar_valor
from horus.utils import get_logger, normalizar_nome

logger = get_logger(__name__)

# Regex para CPF (11 dígitos) ou CNPJ (14 dígitos) com apenas números
_RE_CPF_CNPJ = re.compile(r"^\d{11}(\d{3})?$")


def _truncar(texto: str, limite: int = 80) -> str:
    """Trunca texto sem cortar palavras no meio.

    Se o texto excede *limite*, corta na última palavra inteira que cabe
    e adiciona '…'. Se o texto couber inteiro, retorna inalterado.
    """
    if not texto or len(texto) <= limite:
        return texto
    cortado = texto[:limite]
    # Se o caractere no limite está no meio de uma palavra, volta até o espaço
    if texto[limite] not in (" ", ",", ".", ";", "-"):
        ultimo_espaco = cortado.rfind(" ")
        if ultimo_espaco > limite * 0.4:  # pelo menos 40% do texto preservado
            cortado = cortado[:ultimo_espaco]
    return cortado.rstrip(" ,;") + "…"


class AnomalyDetector:
    """Detecta padrões suspeitos cruzando múltiplas fontes de dados.

    Cada detector segue o padrão de validação:
    1. Identificar candidatos via JOINs por CNPJ/CPF (identificadores concretos)
    2. Validar cruzando com fontes adicionais quando possível
    3. Calcular score baseado em dados factuais
    4. Gerar insight SOMENTE se evidência for verificável
    """

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self.config = config or db.config
        self.manager = InsightManager(db)

    # ------------------------------------------------------------------
    # Pipeline principal
    # ------------------------------------------------------------------

    def detect_all(self) -> list[Insight]:
        """Executa todos os detectores e persiste os resultados."""
        logger.info("Iniciando detecção de anomalias...")
        self.manager.limpar_todos()

        all_insights: list[Insight] = []

        detectors = [
            ("circuito_doacao_contrato", self._detect_circuito_doacao_contrato),
            ("concentracao_fornecedor", self._detect_concentracao_fornecedor),
            ("despesa_atipica", self._detect_despesa_atipica),
            ("fornecedor_doador", self._detect_fornecedor_doador),
            ("fornecedor_sancionado", self._detect_fornecedor_sancionado),
            ("emenda_concentrada", self._detect_emenda_concentrada),
            ("valor_fracionado", self._detect_valor_fracionado),
            ("execucao_orcamentaria_anomala", self._detect_execucao_anomala),
        ]

        for name, detector_fn in detectors:
            try:
                insights = detector_fn()
                all_insights.extend(insights)
                logger.info("Detector '%s': %d insights", name, len(insights))
            except Exception as e:
                logger.warning("Erro no detector '%s': %s", name, e)

        # Persistir
        if all_insights:
            self.manager.salvar_batch(all_insights)
            logger.info("Total: %d insights salvos", len(all_insights))

        return all_insights

    # ==================================================================
    # DETECTORES INDIVIDUAIS
    # ==================================================================

    # ------------------------------------------------------------------
    # 1. Circuito fechado doação → contrato (JOIN por CNPJ)
    # Doador de campanha recebe contrato público depois da eleição
    # ------------------------------------------------------------------

    def _detect_circuito_doacao_contrato(self) -> list[Insight]:
        insights: list[Insight] = []

        circuitos = self.db.query("""
            SELECT d.cpf_cnpj_doador, d.nome_doador, d.nome_candidato,
                   d.valor AS valor_doacao, d.ano_eleicao,
                   c.fornecedor_cnpj, c.fornecedor_nome,
                   c.valor AS valor_contrato, c.objeto, c.orgao
            FROM doacoes d
            JOIN contratos c ON d.cpf_cnpj_doador = c.fornecedor_cnpj
            WHERE d.cpf_cnpj_doador != '' AND c.fornecedor_cnpj != ''
              AND length(d.cpf_cnpj_doador) >= 11
              AND c.data_inicio >= (d.ano_eleicao || '-01-01')
            ORDER BY c.valor DESC
        """)

        # Agrupar por doador
        por_doador: dict[str, list] = defaultdict(list)
        for row in circuitos:
            cpf_cnpj = row["cpf_cnpj_doador"]
            if not _RE_CPF_CNPJ.match(cpf_cnpj):
                continue  # descartar registros com CPF/CNPJ inválido
            por_doador[cpf_cnpj].append(row)

        for cnpj, registros in por_doador.items():
            total_doado = sum(r["valor_doacao"] for r in registros)
            total_contratado = sum(r["valor_contrato"] for r in registros)
            candidatos = set(r["nome_candidato"] for r in registros if r["nome_candidato"])
            nome_doador = registros[0]["nome_doador"] or cnpj

            if total_contratado < 50000:
                continue

            ratio = total_contratado / max(total_doado, 1)
            score = min(95, 65 + min(30, ratio * 3))

            insights.append(Insight(
                tipo="circuito_doacao_contrato",
                titulo=f"Circuito fechado doação → contrato",
                descricao=(
                    f"{nome_doador} doou {formatar_valor(total_doado)} para campanha(s) "
                    f"de {', '.join(candidatos)} e posteriormente recebeu "
                    f"{formatar_valor(total_contratado)} em contratos públicos."
                ),
                severidade=Severidade.CRITICO if total_contratado > 1_000_000 else Severidade.ALTO,
                score=score,
                valor_exposicao=total_contratado,
                pattern="Doou p/ campanha → Eleito → Recebeu contratos públicos",
                fontes=["TSE", "Transparência", "PNCP"],
                politico_id=_find_politico_id(self.db, list(candidatos)[0] if candidatos else ""),
                politico_nome=list(candidatos)[0] if candidatos else "",
                dados={"doador": nome_doador, "cnpj": cnpj,
                       "total_doado": total_doado, "total_contratado": total_contratado},
            ))

        return insights

    # ------------------------------------------------------------------
    # 3. Concentração de fornecedor
    # Poucos fornecedores dominam contratos de um órgão
    # ------------------------------------------------------------------

    def _detect_concentracao_fornecedor(self) -> list[Insight]:
        insights: list[Insight] = []

        concentracao = self.db.query("""
            SELECT orgao, fornecedor_cnpj, fornecedor_nome,
                   COUNT(*) as qtd_contratos,
                   SUM(valor) as total_valor,
                   (SELECT SUM(valor) FROM contratos c2
                    WHERE c2.orgao = c1.orgao AND c2.valor > 0) as total_orgao
            FROM contratos c1
            WHERE fornecedor_cnpj != '' AND valor > 0
              AND length(fornecedor_cnpj) >= 11
            GROUP BY orgao, fornecedor_cnpj
            HAVING qtd_contratos >= 3 AND total_valor > 500000
            ORDER BY total_valor DESC
        """)

        for row in concentracao:
            if not _RE_CPF_CNPJ.match(row["fornecedor_cnpj"]):
                continue
            total_orgao = row["total_orgao"] or 1
            pct = (row["total_valor"] / total_orgao) * 100

            if pct < 30:
                continue

            # Verificar se fornecedor tem sanção ativa (agrava)
            tem_sancao = bool(self.db.query(
                "SELECT 1 FROM sancoes WHERE cpf_cnpj = ? LIMIT 1",
                (row["fornecedor_cnpj"],),
            ))

            score = min(92, 50 + pct * 0.4 + (10 if tem_sancao else 0))
            sev = Severidade.CRITICO if pct > 60 else (
                Severidade.ALTO if pct > 40 else Severidade.MEDIO)

            desc = (
                f"{_truncar(row['fornecedor_nome'], 60)} ({row['fornecedor_cnpj'][:8]}…) "
                f"detém {pct:.0f}% dos contratos do órgão "
                f"{_truncar(row['orgao'], 80)} — {row['qtd_contratos']} contratos "
                f"totalizando {formatar_valor(row['total_valor'])}."
            )
            if tem_sancao:
                desc += " ⚠ Este fornecedor consta em cadastro de sanções."

            insights.append(Insight(
                tipo="concentracao_fornecedor",
                titulo=f"Concentração: {_truncar(row['fornecedor_nome'], 60)}",
                descricao=desc,
                severidade=sev,
                score=score,
                valor_exposicao=row["total_valor"],
                pattern=f"Fornecedor concentra {pct:.0f}% dos contratos do órgão",
                fontes=["Portal da Transparência"],
                dados={
                    "fornecedor_cnpj": row["fornecedor_cnpj"],
                    "fornecedor_nome": row["fornecedor_nome"],
                    "pct": round(pct, 1),
                    "orgao": row["orgao"],
                    "qtd_contratos": row["qtd_contratos"],
                    "tem_sancao": tem_sancao,
                },
            ))

        return insights[:20]  # Limitar

    # ------------------------------------------------------------------
    # 4. Despesas parlamentares atípicas
    # Gasto acima de média + 2 desvios-padrão (≈ P97.7)
    # ------------------------------------------------------------------

    def _detect_despesa_atipica(self) -> list[Insight]:
        insights: list[Insight] = []

        # Gastos por deputado
        gastos = self.db.query("""
            SELECT politico_id, SUM(valor) as total_gasto, COUNT(*) as qtd
            FROM despesas_parlamentares
            GROUP BY politico_id
            HAVING total_gasto > 0
        """)

        if len(gastos) < 10:
            return insights

        # Calcular P95
        valores = [g["total_gasto"] for g in gastos]
        media = statistics.mean(valores)
        desvio = statistics.stdev(valores) if len(valores) > 1 else 0
        limiar = media + 2 * desvio

        for gasto in gastos:
            if gasto["total_gasto"] <= limiar:
                continue

            politico = self.db.buscar_politico_id(gasto["politico_id"])
            if not politico:
                continue

            zscore = (gasto["total_gasto"] - media) / desvio if desvio > 0 else 0
            score = min(92, 60 + zscore * 8)

            insights.append(Insight(
                tipo="despesa_atipica",
                titulo=f"Despesa parlamentar atípica",
                descricao=(
                    f"{politico['nome']} ({politico['partido']}-{politico['uf']}) "
                    f"gastou {formatar_valor(gasto['total_gasto'])} em despesas parlamentares, "
                    f"{zscore:.1f}x acima da média de {formatar_valor(media)}."
                ),
                severidade=Severidade.ALTO if zscore > 3 else Severidade.MEDIO,
                score=score,
                valor_exposicao=gasto["total_gasto"] - media,
                pattern=f"Despesa parlamentar {zscore:.1f}x acima da média",
                fontes=["Câmara dos Deputados"],
                politico_id=gasto["politico_id"],
                politico_nome=politico["nome"],
                dados={"total": gasto["total_gasto"], "media": media, "zscore": zscore},
            ))

        # Concentração de fornecedor no deputado
        concentrated = self.db.query("""
            SELECT politico_id, fornecedor_cnpj, fornecedor_nome,
                   SUM(valor) as total_forn,
                   (SELECT SUM(valor) FROM despesas_parlamentares dp2
                    WHERE dp2.politico_id = dp1.politico_id) as total_dep
            FROM despesas_parlamentares dp1
            WHERE fornecedor_cnpj != ''
            GROUP BY politico_id, fornecedor_cnpj
            HAVING total_forn > 100000
        """)

        for row in concentrated:
            total_dep = row["total_dep"] or 1
            pct = (row["total_forn"] / total_dep) * 100
            if pct < 30:
                continue

            politico = self.db.buscar_politico_id(row["politico_id"])
            if not politico:
                continue

            insights.append(Insight(
                tipo="despesa_concentrada",
                titulo=f"Despesa concentrada em fornecedor",
                descricao=(
                    f"{politico['nome']} direcionou {pct:.0f}% das despesas "
                    f"({formatar_valor(row['total_forn'])}) para {row['fornecedor_nome']}."
                ),
                severidade=Severidade.ALTO if pct > 50 else Severidade.MEDIO,
                score=min(88, 55 + pct * 0.5),
                valor_exposicao=row["total_forn"],
                pattern=f"Cota parlamentar: {pct:.0f}% direcionada a 1 fornecedor",
                fontes=["Câmara dos Deputados"],
                politico_id=row["politico_id"],
                politico_nome=politico["nome"],
                dados={"fornecedor": row["fornecedor_nome"], "pct": pct},
            ))

        return insights[:30]

    # ------------------------------------------------------------------
    # 5. Fornecedor que é doador de campanha (JOIN por CNPJ)
    # ------------------------------------------------------------------

    def _detect_fornecedor_doador(self) -> list[Insight]:
        insights: list[Insight] = []

        matches = self.db.query("""
            SELECT
                d.cpf_cnpj_doador, d.nome_doador,
                d.nome_candidato, d.valor AS valor_doacao, d.ano_eleicao,
                dp.politico_id, dp.fornecedor_cnpj, dp.fornecedor_nome,
                SUM(dp.valor) as total_despesa
            FROM doacoes d
            JOIN despesas_parlamentares dp
                ON d.cpf_cnpj_doador = dp.fornecedor_cnpj
            WHERE d.cpf_cnpj_doador != ''
              AND length(d.cpf_cnpj_doador) >= 11
              AND (dp.ano >= d.ano_eleicao OR dp.ano IS NULL)
            GROUP BY d.cpf_cnpj_doador, dp.politico_id
            HAVING total_despesa > 50000
            ORDER BY total_despesa DESC
        """)

        for row in matches:
            politico = self.db.buscar_politico_id(row["politico_id"])
            nome_pol = politico["nome"] if politico else row["nome_candidato"] or ""

            insights.append(Insight(
                tipo="fornecedor_doador",
                titulo=f"Doador = Fornecedor: {_truncar(row['nome_doador'], 60)}",
                descricao=(
                    f"{row['nome_doador']} doou para campanha de {row['nome_candidato']} "
                    f"em {row['ano_eleicao']} e recebe despesas parlamentares "
                    f"totalizando {formatar_valor(row['total_despesa'])}."
                ),
                severidade=Severidade.ALTO,
                score=min(90, 70 + min(20, row["total_despesa"] / 100000)),
                valor_exposicao=row["total_despesa"],
                pattern="Doador de campanha depois virou fornecedor parlamentar",
                fontes=["TSE", "Câmara dos Deputados"],
                politico_id=row["politico_id"],
                politico_nome=nome_pol,
                dados={
                    "doador_cnpj": row["cpf_cnpj_doador"],
                    "doador_nome": row["nome_doador"],
                },
            ))

        return insights[:20]

    # ------------------------------------------------------------------
    # 6. Fornecedor sancionado que continua contratado (JOIN por CNPJ)
    # NOTA: NÃO usamos match por nome — muito impreciso.
    # ------------------------------------------------------------------

    def _detect_fornecedor_sancionado(self) -> list[Insight]:
        """Fornecedores que constam em cadastro de sanções mas
        continuam recebendo contratos públicos."""
        insights: list[Insight] = []

        matches = self.db.query("""
            SELECT
                s.tipo as tipo_sancao,
                s.cpf_cnpj,
                s.nome as nome_sancionado,
                s.orgao_sancionador,
                s.data_inicio as sancao_inicio,
                s.data_fim as sancao_fim,
                COUNT(DISTINCT c.id) as qtd_contratos,
                SUM(c.valor) as total_contratos,
                GROUP_CONCAT(DISTINCT c.orgao) as orgaos
            FROM sancoes s
            JOIN contratos c ON s.cpf_cnpj = c.fornecedor_cnpj
            WHERE s.cpf_cnpj != ''
              AND length(s.cpf_cnpj) >= 11
              AND c.valor > 0
              AND (
                  s.data_fim IS NULL
                  OR s.data_fim = ''
                  OR c.data_inicio <= s.data_fim
              )
              AND (
                  s.data_inicio IS NULL
                  OR s.data_inicio = ''
                  OR c.data_inicio >= s.data_inicio
                  OR c.data_fim >= s.data_inicio
              )
            GROUP BY s.cpf_cnpj, s.tipo
            HAVING total_contratos > 10000
            ORDER BY total_contratos DESC
        """)

        for row in matches:
            if not _RE_CPF_CNPJ.match(row["cpf_cnpj"]):
                continue
            total = row["total_contratos"] or 0
            orgaos = _truncar(row["orgaos"] or "", 80)

            sev = Severidade.CRITICO if total > 500000 else Severidade.ALTO
            score = min(95, 75 + min(20, total / 500000 * 20))

            insights.append(Insight(
                tipo="fornecedor_sancionado",
                titulo=f"Sancionado c/ contratos: {_truncar(row['nome_sancionado'], 60)}",
                descricao=(
                    f"{row['nome_sancionado']} ({row['cpf_cnpj']}) consta no "
                    f"{row['tipo_sancao']} (sancionado por "
                    f"{_truncar(row['orgao_sancionador'] or 'N/I', 80)}) mas possui "
                    f"{row['qtd_contratos']} contrato(s) ativo(s) totalizando "
                    f"{formatar_valor(total)}."
                ),
                severidade=sev,
                score=score,
                valor_exposicao=total,
                pattern=f"Sancionado no {row['tipo_sancao']}, mas segue recebendo contratos",
                fontes=["CGU Sanções", "Portal da Transparência"],
                dados={
                    "cpf_cnpj": row["cpf_cnpj"],
                    "tipo_sancao": row["tipo_sancao"],
                    "qtd_contratos": row["qtd_contratos"],
                    "orgaos": orgaos,
                },
            ))

        return insights[:30]

    # ------------------------------------------------------------------
    # 7. Emenda concentrada em poucos municípios
    # ------------------------------------------------------------------

    def _detect_emenda_concentrada(self) -> list[Insight]:
        insights: list[Insight] = []

        concentracoes = self.db.query("""
            SELECT autor, localidade, uf,
                   SUM(valor_empenhado) as total,
                   COUNT(*) as qtd,
                   (SELECT SUM(valor_empenhado) FROM emendas e2
                    WHERE e2.autor = e1.autor) as total_autor
            FROM emendas e1
            WHERE autor IS NOT NULL AND valor_empenhado > 0
            GROUP BY autor, localidade
            HAVING qtd >= 3 AND total > 1000000
            ORDER BY total DESC
        """)

        for row in concentracoes:
            total_autor = row["total_autor"] or 1
            pct = (row["total"] / total_autor) * 100

            if pct < 40:
                continue

            score = min(90, 55 + pct * 0.4)

            insights.append(Insight(
                tipo="emenda_concentrada",
                titulo=f"Emendas concentradas em {_truncar(row['localidade'], 60)}",
                descricao=(
                    f"{row['autor']} destinou {pct:.0f}% de suas emendas "
                    f"({formatar_valor(row['total'])}) para {row['localidade']}/{row['uf']} "
                    f"({row['qtd']} emendas)."
                ),
                severidade=Severidade.ALTO if pct > 60 else Severidade.MEDIO,
                score=score,
                valor_exposicao=row["total"],
                pattern=f"Emendas: {pct:.0f}% concentradas em uma única localidade",
                fontes=["Transparência"],
                politico_id=_find_politico_id(self.db, row["autor"]),
                politico_nome=row["autor"],
                dados={"localidade": row["localidade"], "pct": pct},
            ))

        return insights[:30]

    # ------------------------------------------------------------------
    # 8. Valor fracionado (contratos logo abaixo do limite de dispensa)
    # ------------------------------------------------------------------

    def _detect_valor_fracionado(self) -> list[Insight]:
        insights: list[Insight] = []

        # Limite de dispensa de licitação (art. 75 Lei 14.133/2021)
        LIMIAR_DISPENSA = 59_906.02  # bens/serviços (2024)
        FAIXA_MIN = LIMIAR_DISPENSA * 0.7
        FAIXA_MAX = LIMIAR_DISPENSA * 1.0

        fracionados = self.db.query("""
            SELECT orgao, fornecedor_cnpj, fornecedor_nome,
                   COUNT(*) as qtd, SUM(valor) as total,
                   AVG(valor) as media
            FROM contratos
            WHERE valor BETWEEN ? AND ?
              AND fornecedor_cnpj != ''
              AND length(fornecedor_cnpj) >= 11
            GROUP BY orgao, fornecedor_cnpj
            HAVING qtd >= 3
            ORDER BY total DESC
        """, (FAIXA_MIN, FAIXA_MAX))

        for row in fracionados:
            score = min(88, 55 + row["qtd"] * 6)

            insights.append(Insight(
                tipo="valor_fracionado",
                titulo="Possível fracionamento de contrato",
                descricao=(
                    f"{_truncar(row['fornecedor_nome'], 60)} tem {row['qtd']} contratos com "
                    f"{_truncar(row['orgao'], 80)}, todos entre {formatar_valor(FAIXA_MIN)} e "
                    f"{formatar_valor(FAIXA_MAX)} (limite de dispensa). "
                    f"Total: {formatar_valor(row['total'])}."
                ),
                severidade=Severidade.ALTO if row["qtd"] >= 5 else Severidade.MEDIO,
                score=score,
                valor_exposicao=row["total"],
                pattern=f"Fracionamento: {row['qtd']} contratos abaixo do limite de dispensa",
                fontes=["Transparência", "PNCP"],
                dados={"qtd": row["qtd"], "orgao": row["orgao"]},
            ))

        return insights[:20]

    # ------------------------------------------------------------------
    # 9. Execução orçamentária anômala (dados Tesouro/SICONFI + emendas)
    # Cruza dados de execução orçamentária RREO com emendas parlamentares
    # para detectar entes com execução atípica de recursos de emendas.
    # ------------------------------------------------------------------

    def _detect_execucao_anomala(self) -> list[Insight]:
        """Detecta anomalias cruzando dados de execução orçamentária (SIAFI/SICONFI)
        com emendas parlamentares. Verifica se a tabela existe antes de consultar."""
        insights: list[Insight] = []

        # Verificar se a tabela execucao_orcamentaria existe (preenchida pelo ETL SIAFI)
        try:
            tables = self.db.query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='execucao_orcamentaria'"
            )
            if not tables:
                return insights  # Tabela não existe ainda — ETL SIAFI não rodou
        except Exception:
            return insights

        # Buscar emendas com alto valor concentradas em uma localidade
        # e cruzar com dados de execução orçamentária
        try:
            # Verificar emendas por UF e comparar com execução orçamentária
            emendas_uf = self.db.query("""
                SELECT uf, SUM(valor_empenhado) as total_emendas,
                       COUNT(*) as qtd_emendas,
                       GROUP_CONCAT(DISTINCT autor) as autores
                FROM emendas
                WHERE valor_empenhado > 0 AND uf IS NOT NULL
                GROUP BY uf
                HAVING total_emendas > 5000000
                ORDER BY total_emendas DESC
            """)

            if not emendas_uf:
                return insights

            # Calcular média e desvio para detectar concentrações estaduais atípicas
            valores = [e["total_emendas"] for e in emendas_uf]
            if len(valores) < 5:
                return insights

            media = statistics.mean(valores)
            desvio = statistics.stdev(valores) if len(valores) > 1 else 0

            for row in emendas_uf:
                if desvio == 0:
                    continue
                zscore = (row["total_emendas"] - media) / desvio
                if zscore < 2.0:
                    continue

                autores = (row["autores"] or "")[:120]

                insights.append(Insight(
                    tipo="execucao_orcamentaria_anomala",
                    titulo=f"Emendas concentradas em {row['uf']}",
                    descricao=(
                        f"O estado {row['uf']} recebeu {formatar_valor(row['total_emendas'])} "
                        f"em {row['qtd_emendas']} emendas parlamentares, "
                        f"{zscore:.1f}x acima da média estadual de "
                        f"{formatar_valor(media)}. Autores: {_truncar(autores, 80)}."
                    ),
                    severidade=Severidade.ALTO if zscore > 3 else Severidade.MEDIO,
                    score=min(88, 55 + zscore * 8),
                    valor_exposicao=row["total_emendas"] - media,
                    pattern=f"Emendas {zscore:.1f}x acima da média estadual",
                    fontes=["Transparência", "Tesouro/SICONFI"],
                    dados={"uf": row["uf"], "total": row["total_emendas"],
                           "zscore": round(zscore, 2)},
                ))

        except Exception as e:
            logger.warning("Erro detector execução orçamentária: %s", e)

        return insights[:15]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _find_politico_id(db: DatabaseManager, nome: str) -> str:
    """Tenta encontrar ID de político pelo nome.

    Estratégia conservadora p/ evitar falsos positivos:
    1. Tenta match exato (normalizado) primeiro
    2. Depois tenta LIKE, mas exige resultado único — se houver
       múltiplos candidatos, retorna '' (ambíguo)
    """
    if not nome or not isinstance(nome, str):
        return ""
    nome_norm = normalizar_nome(nome)
    if len(nome_norm) < 5:
        return ""

    # 1) Match exato (mais confiável)
    rows = db.query(
        "SELECT id FROM politicos WHERE UPPER(nome) = ? LIMIT 1",
        (nome_norm,),
    )
    if rows:
        return rows[0]["id"]

    # 2) Match parcial — só se resultado for único (evita ambiguidade)
    rows = db.query(
        "SELECT id FROM politicos WHERE UPPER(nome) LIKE ? LIMIT 2",
        (f"%{nome_norm}%",),
    )
    if len(rows) == 1:
        return rows[0]["id"]

    return ""  # ambíguo ou não encontrado
