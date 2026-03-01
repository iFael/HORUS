"""Motor de Scoring de Risco — 25+ indicadores ponderados."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from horus.config import Config
from horus.database import DatabaseManager
from horus.graph_builder import EdgeType, GraphBuilder, NodeType
from horus.utils import (
    get_logger,
    limpar_documento,
    mesmo_sobrenome,
    normalizar_nome,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Resultado
# ---------------------------------------------------------------------------

@dataclass
class IndicadorRisco:
    """Resultado de um indicador individual."""
    nome: str
    peso: float
    score: float  # 0.0 – 1.0 (normalizado)
    contribuicao: float  # peso * score
    detalhes: str = ""
    evidencias: list[str] = field(default_factory=list)


@dataclass
class ResultadoRisco:
    """Resultado completo da análise de risco."""
    cpf_cnpj: str
    nome: str
    score_total: float  # 0 – 100
    nivel: str  # Baixo / Médio / Alto / Muito Alto
    indicadores: list[IndicadorRisco] = field(default_factory=list)
    resumo: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def nivel_calculado(self) -> str:
        if self.score_total <= 25:
            return "Baixo"
        elif self.score_total <= 50:
            return "Médio"
        elif self.score_total <= 75:
            return "Alto"
        return "Muito Alto"

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpf_cnpj": self.cpf_cnpj,
            "nome": self.nome,
            "score_total": round(self.score_total, 2),
            "nivel": self.nivel,
            "indicadores": [
                {
                    "nome": i.nome,
                    "peso": i.peso,
                    "score": round(i.score, 4),
                    "contribuicao": round(i.contribuicao, 4),
                    "detalhes": i.detalhes,
                    "evidencias": i.evidencias,
                }
                for i in self.indicadores
            ],
            "resumo": self.resumo,
            "timestamp": self.timestamp,
        }

    def to_markdown(self) -> str:
        lines = [
            f"# Análise de Risco — {self.nome}",
            f"**CPF/CNPJ:** {self.cpf_cnpj}",
            f"**Score Total:** {self.score_total:.1f}/100",
            f"**Nível:** {self.nivel}",
            f"**Data:** {self.timestamp[:10]}",
            "",
            "## Indicadores",
            "",
            "| Indicador | Peso | Score | Contribuição | Detalhes |",
            "|-----------|------|-------|-------------|----------|",
        ]
        for i in sorted(self.indicadores, key=lambda x: -x.contribuicao):
            lines.append(
                f"| {i.nome} | {i.peso:.0f} | {i.score:.2f} | "
                f"{i.contribuicao:.2f} | {i.detalhes[:60]} |"
            )
        lines.extend([
            "",
            "## Resumo",
            "",
            self.resumo,
        ])
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class RiskEngine:
    """Calcula score de risco para CPF ou CNPJ."""

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self.config = config or db.config
        self.pesos = self.config.risk.pesos
        self.graph_builder = GraphBuilder(db, self.config)

    # ===================================================================
    # API pública
    # ===================================================================

    def calcular_risco_cpf(self, cpf: str) -> ResultadoRisco:
        """Calcula risco para uma pessoa física."""
        cpf = limpar_documento(cpf)
        pessoa = self.db.buscar_pessoa_cpf(cpf)
        nome = pessoa["nome"] if pessoa else "Desconhecido"

        # Construir grafo
        self.graph_builder.build_from_cpf(cpf, profundidade=2)
        self.graph_builder.add_same_address_edges()
        self.graph_builder.add_family_edges()

        indicadores = self._avaliar_indicadores_cpf(cpf)
        score_total = self._calcular_score(indicadores)
        resultado = ResultadoRisco(
            cpf_cnpj=cpf,
            nome=nome,
            score_total=score_total,
            nivel="",
            indicadores=indicadores,
        )
        resultado.nivel = resultado.nivel_calculado
        resultado.resumo = self._gerar_resumo(resultado)
        return resultado

    def calcular_risco_cnpj(self, cnpj: str) -> ResultadoRisco:
        """Calcula risco para uma empresa."""
        cnpj = limpar_documento(cnpj)
        empresa = self.db.buscar_empresa_cnpj(cnpj)
        nome = empresa["razao_social"] if empresa else "Desconhecido"

        self.graph_builder.build_from_cnpj(cnpj, profundidade=2)
        self.graph_builder.add_same_address_edges()

        indicadores = self._avaliar_indicadores_cnpj(cnpj)
        score_total = self._calcular_score(indicadores)
        resultado = ResultadoRisco(
            cpf_cnpj=cnpj,
            nome=nome,
            score_total=score_total,
            nivel="",
            indicadores=indicadores,
        )
        resultado.nivel = resultado.nivel_calculado
        resultado.resumo = self._gerar_resumo(resultado)
        return resultado

    # ===================================================================
    # Cálculo do score final
    # ===================================================================

    def _calcular_score(self, indicadores: list[IndicadorRisco]) -> float:
        """Média ponderada normalizada para 0–100."""
        if not indicadores:
            return 0.0
        total_peso = sum(i.peso for i in indicadores)
        if total_peso == 0:
            return 0.0
        total_contrib = sum(i.contribuicao for i in indicadores)
        return min(100.0, (total_contrib / total_peso) * 100)

    # ===================================================================
    # Indicadores para CPF
    # ===================================================================

    def _avaliar_indicadores_cpf(self, cpf: str) -> list[IndicadorRisco]:
        indicadores: list[IndicadorRisco] = []

        indicadores.append(self._indicador_sancao_ativa(cpf))
        indicadores.append(self._indicador_acumulacao_cargos(cpf))
        indicadores.append(self._indicador_empresa_familiar_contrato(cpf))
        indicadores.append(self._indicador_variacao_patrimonial(cpf))
        indicadores.append(self._indicador_emenda_autodirecionada(cpf))
        indicadores.append(self._indicador_doador_contratado(cpf))
        indicadores.append(self._indicador_parente_fornecedor(cpf))

        return [i for i in indicadores if i is not None]

    def _avaliar_indicadores_cnpj(self, cnpj: str) -> list[IndicadorRisco]:
        indicadores: list[IndicadorRisco] = []

        indicadores.append(self._indicador_sancao_ativa(cnpj))
        indicadores.append(self._indicador_empresa_recem_criada(cnpj))
        indicadores.append(self._indicador_concentracao_contratos(cnpj))
        indicadores.append(self._indicador_mesmo_endereco(cnpj))
        indicadores.append(self._indicador_inexigibilidade_alta(cnpj))
        indicadores.append(self._indicador_aditivo_excessivo(cnpj))
        indicadores.append(self._indicador_socio_pep(cnpj))

        return [i for i in indicadores if i is not None]

    # ===================================================================
    # Implementação dos indicadores
    # ===================================================================

    def _indicador_sancao_ativa(self, cpf_cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("sancao_ativa", 20)
        sancoes = self.db.buscar_sancoes(cpf_cnpj)
        ativas = [
            s for s in sancoes
            if not s.get("data_fim") or s["data_fim"] >= datetime.now().isoformat()[:10]
        ]
        score = min(1.0, len(ativas) * 0.5)
        evidencias = [f"{s['tipo']}: {s.get('orgao_sancionador', 'N/A')}" for s in ativas[:5]]
        return IndicadorRisco(
            nome="Sanção ativa",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{len(ativas)} sanção(ões) ativa(s)" if ativas else "Sem sanções ativas",
            evidencias=evidencias,
        )

    def _indicador_acumulacao_cargos(self, cpf: str) -> IndicadorRisco:
        peso = self.pesos.get("acumulacao_cargos", 5)
        servidores = self.db.buscar_servidores_cpf(cpf)
        orgaos_distintos = len({s.get("orgao_cnpj", s.get("orgao", "")) for s in servidores})
        score = min(1.0, max(0, orgaos_distintos - 1) * 0.5)
        return IndicadorRisco(
            nome="Acumulação de cargos",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"Servidor em {orgaos_distintos} órgão(s)",
            evidencias=[s.get("orgao", "") for s in servidores[:5]],
        )

    def _indicador_empresa_familiar_contrato(self, cpf: str) -> IndicadorRisco:
        peso = self.pesos.get("empresa_familiar_contrato", 15)
        pessoa = self.db.buscar_pessoa_cpf(cpf)
        if not pessoa:
            return IndicadorRisco(nome="Empresa familiar contratada", peso=peso,
                                  score=0, contribuicao=0, detalhes="Pessoa não encontrada")

        nome = pessoa.get("nome", "")
        empresas_socio = self.db.buscar_empresas_socio(cpf)

        hits = 0
        evidencias: list[str] = []
        for emp in empresas_socio:
            cnpj = emp.get("cnpj", "")
            if not cnpj:
                continue
            socios = self.db.buscar_socios_empresa(cnpj)
            for s in socios:
                socio_nome = s.get("nome_socio", "")
                if socio_nome and nome and mesmo_sobrenome(nome, socio_nome):
                    contratos = self.db.buscar_contratos_fornecedor(cnpj)
                    if contratos:
                        hits += 1
                        evidencias.append(
                            f"CNPJ {cnpj}: sócio {socio_nome} (mesmo sobrenome), "
                            f"{len(contratos)} contrato(s)"
                        )

        score = min(1.0, hits * 0.4)
        return IndicadorRisco(
            nome="Empresa familiar contratada",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{hits} empresa(s) familiar(es) com contratos" if hits else "Nenhum padrão encontrado",
            evidencias=evidencias[:5],
        )

    def _indicador_variacao_patrimonial(self, cpf: str) -> IndicadorRisco:
        peso = self.pesos.get("variacao_patrimonial", 8)
        candidaturas = self.db.buscar_candidaturas(cpf)
        if len(candidaturas) < 2:
            return IndicadorRisco(
                nome="Variação patrimonial",
                peso=peso, score=0, contribuicao=0,
                detalhes="Menos de 2 candidaturas para comparação"
            )

        bens_por_ano = sorted(
            [(c.get("ano_eleicao", 0), float(c.get("total_bens", 0) or 0)) for c in candidaturas],
            key=lambda x: x[0]
        )

        max_variacao = 0.0
        for i in range(1, len(bens_por_ano)):
            anterior = bens_por_ano[i - 1][1]
            if anterior > 0:
                variacao = (bens_por_ano[i][1] - anterior) / anterior
                max_variacao = max(max_variacao, variacao)

        limiar = self.config.risk.variacao_patrimonial_limiar
        score = min(1.0, max(0, max_variacao / limiar)) if max_variacao > 1.0 else 0.0
        return IndicadorRisco(
            nome="Variação patrimonial",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"Maior variação: {max_variacao * 100:.0f}%",
            evidencias=[f"{ano}: R$ {val:,.2f}" for ano, val in bens_por_ano],
        )

    def _indicador_emenda_autodirecionada(self, cpf: str) -> IndicadorRisco:
        peso = self.pesos.get("emenda_autodirecionada", 8)
        emendas = self.db.buscar_emendas_autor(cpf)
        candidaturas = self.db.buscar_candidaturas(cpf)

        if not emendas or not candidaturas:
            return IndicadorRisco(
                nome="Emenda autodirecionada", peso=peso,
                score=0, contribuicao=0, detalhes="Sem emendas ou candidaturas"
            )

        domicilios = {normalizar_nome(c.get("municipio", "")) for c in candidaturas if c.get("municipio")}
        emendas_local = [
            e for e in emendas
            if normalizar_nome(e.get("localidade", "")) in domicilios
        ]

        ratio = len(emendas_local) / len(emendas) if emendas else 0
        score = min(1.0, max(0, (ratio - 0.3) / 0.4)) if ratio > 0.3 else 0.0

        return IndicadorRisco(
            nome="Emenda autodirecionada",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{len(emendas_local)}/{len(emendas)} emendas no domicílio eleitoral ({ratio * 100:.0f}%)",
        )

    def _indicador_doador_contratado(self, cpf: str) -> IndicadorRisco:
        peso = self.pesos.get("doador_contratado", 12)
        doacoes = self.db.buscar_doacoes_candidato(cpf)
        if not doacoes:
            return IndicadorRisco(
                nome="Doador contratado", peso=peso,
                score=0, contribuicao=0, detalhes="Sem doações recebidas"
            )

        doadores_cnpj = {limpar_documento(d.get("cpf_cnpj_doador", "")) for d in doacoes
                         if len(limpar_documento(d.get("cpf_cnpj_doador", ""))) == 14}

        hits = 0
        evidencias: list[str] = []
        for cnpj in doadores_cnpj:
            contratos = self.db.buscar_contratos_fornecedor(cnpj)
            if contratos:
                hits += 1
                evidencias.append(f"CNPJ {cnpj}: doou + tem {len(contratos)} contrato(s)")

        score = min(1.0, hits * 0.3)
        return IndicadorRisco(
            nome="Doador contratado",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{hits} doador(es) com contratos públicos" if hits else "Nenhum doador contratado",
            evidencias=evidencias[:5],
        )

    def _indicador_parente_fornecedor(self, cpf: str) -> IndicadorRisco:
        peso = self.pesos.get("parente_fornecedor", 8)
        G = self.graph_builder.graph
        nid = f"{NodeType.PESSOA}:{cpf}"

        if nid not in G:
            return IndicadorRisco(nome="Parente fornecedor", peso=peso,
                                  score=0, contribuicao=0, detalhes="Nó não encontrado")

        hits = 0
        evidencias: list[str] = []
        for _, vizinho, data in G.edges(nid, data=True):
            if data.get("tipo") == EdgeType.PARENTE_DE:
                # Verifica se o parente é sócio de empresa com contrato
                for _, emp, edata in G.edges(vizinho, data=True):
                    if edata.get("tipo") == EdgeType.SOCIO_DE:
                        emp_data = G.nodes.get(emp, {})
                        if emp_data.get("tipo") == NodeType.EMPRESA:
                            cnpj = emp_data.get("identificador", "")
                            contratos = self.db.buscar_contratos_fornecedor(cnpj)
                            if contratos:
                                hits += 1
                                evidencias.append(
                                    f"Parente sócio de {cnpj} com {len(contratos)} contrato(s)"
                                )

        score = min(1.0, hits * 0.4)
        return IndicadorRisco(
            nome="Parente fornecedor",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{hits} parente(s) sócios de empresas contratadas" if hits else "Nenhum padrão",
            evidencias=evidencias[:5],
        )

    # ------------------------------------------------------------------
    # Indicadores para CNPJ
    # ------------------------------------------------------------------

    def _indicador_empresa_recem_criada(self, cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("empresa_recem_criada", 10)
        empresa = self.db.buscar_empresa_cnpj(cnpj)
        if not empresa or not empresa.get("data_abertura"):
            return IndicadorRisco(nome="Empresa recém-criada", peso=peso,
                                  score=0, contribuicao=0, detalhes="Data de abertura não disponível")

        try:
            abertura = datetime.fromisoformat(empresa["data_abertura"][:10])
        except (ValueError, TypeError):
            return IndicadorRisco(nome="Empresa recém-criada", peso=peso,
                                  score=0, contribuicao=0, detalhes="Data inválida")

        contratos = self.db.buscar_contratos_fornecedor(cnpj)
        if not contratos:
            return IndicadorRisco(nome="Empresa recém-criada", peso=peso,
                                  score=0, contribuicao=0, detalhes="Sem contratos")

        primeiro_contrato = min(
            (c.get("data_inicio", "") for c in contratos if c.get("data_inicio")),
            default=""
        )
        if not primeiro_contrato:
            return IndicadorRisco(nome="Empresa recém-criada", peso=peso,
                                  score=0, contribuicao=0, detalhes="Data de contrato indisponível")

        try:
            dt_contrato = datetime.fromisoformat(primeiro_contrato[:10])
        except (ValueError, TypeError):
            return IndicadorRisco(nome="Empresa recém-criada", peso=peso,
                                  score=0, contribuicao=0, detalhes="Data inválida")

        idade_anos = (dt_contrato - abertura).days / 365.25
        limiar = self.config.risk.empresa_idade_min
        score = min(1.0, max(0, 1 - idade_anos / limiar)) if idade_anos < limiar else 0.0

        return IndicadorRisco(
            nome="Empresa recém-criada",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"Empresa com {idade_anos:.1f} anos no primeiro contrato (limiar: {limiar})",
        )

    def _indicador_concentracao_contratos(self, cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("concentracao_contratos", 12)
        contratos = self.db.buscar_contratos_fornecedor(cnpj)
        if not contratos:
            return IndicadorRisco(nome="Concentração de contratos", peso=peso,
                                  score=0, contribuicao=0, detalhes="Sem contratos")

        # Verificar se o CNPJ concentra contratos em 1 órgão
        orgaos: dict[str, int] = {}
        for c in contratos:
            org = c.get("orgao_cnpj", c.get("orgao", ""))
            if org:
                orgaos[org] = orgaos.get(org, 0) + 1

        if not orgaos:
            return IndicadorRisco(nome="Concentração de contratos", peso=peso,
                                  score=0, contribuicao=0, detalhes="Dados insuficientes")

        max_count = max(orgaos.values())
        total = sum(orgaos.values())
        ratio = max_count / total if total else 0.0

        limiar = self.config.risk.concentracao_limiar
        score = min(1.0, max(0, (ratio - limiar) / (1 - limiar))) if ratio > limiar else 0.0

        return IndicadorRisco(
            nome="Concentração de contratos",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{ratio * 100:.0f}% dos contratos em um único órgão ({max_count}/{total})",
        )

    def _indicador_mesmo_endereco(self, cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("mesmo_endereco_multiplos_cnpj", 10)
        G = self.graph_builder.graph
        nid = f"{NodeType.EMPRESA}:{cnpj}"

        if nid not in G:
            return IndicadorRisco(nome="Mesmo endereço (múltiplos CNPJs)", peso=peso,
                                  score=0, contribuicao=0, detalhes="Nó não encontrado")

        vizinhos_endereco = [
            v for _, v, d in G.edges(nid, data=True)
            if d.get("tipo") == EdgeType.MESMO_ENDERECO
        ]
        count = len(vizinhos_endereco)
        score = min(1.0, count * 0.3) if count >= 2 else 0.0

        return IndicadorRisco(
            nome="Mesmo endereço (múltiplos CNPJs)",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{count + 1} empresa(s) no mesmo endereço" if count else "Endereço único",
            evidencias=[G.nodes[v].get("nome", v) for v in vizinhos_endereco[:5]],
        )

    def _indicador_inexigibilidade_alta(self, cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("inexigibilidade_alta", 7)
        contratos = self.db.buscar_contratos_fornecedor(cnpj)

        inex = [
            c for c in contratos
            if "inexig" in (c.get("modalidade", "") or "").lower()
        ]

        valor_total = sum(float(c.get("valor", 0) or 0) for c in inex)
        limiar = self.config.risk.inexigibilidade_valor_min
        score = min(1.0, valor_total / (limiar * 3)) if valor_total > limiar else 0.0

        return IndicadorRisco(
            nome="Inexigibilidade alta",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"R$ {valor_total:,.2f} em inexigibilidades ({len(inex)} contrato(s))",
        )

    def _indicador_aditivo_excessivo(self, cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("aditivo_excessivo", 7)
        # Indicador simplificado — detecta contratos com valor muito acima da licitação
        contratos = self.db.buscar_contratos_fornecedor(cnpj)
        if not contratos:
            return IndicadorRisco(nome="Aditivo excessivo", peso=peso,
                                  score=0, contribuicao=0, detalhes="Sem contratos")

        # Aqui seria ideal comparar valor contrato vs valor estimado da licitação
        # Simplificação: contratos muito grandes (>R$10M) pontuam levemente
        valores = [float(c.get("valor", 0) or 0) for c in contratos]
        max_valor = max(valores) if valores else 0
        score = min(1.0, max(0, max_valor / 10_000_000 - 1) * 0.3) if max_valor > 10_000_000 else 0.0

        return IndicadorRisco(
            nome="Aditivo excessivo",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"Maior contrato: R$ {max_valor:,.2f}",
        )

    def _indicador_socio_pep(self, cnpj: str) -> IndicadorRisco:
        peso = self.pesos.get("empresa_socio_pep", 10)
        socios = self.db.buscar_socios_empresa(cnpj)

        pep_count = 0
        evidencias: list[str] = []
        for s in socios:
            socio_id = limpar_documento(s.get("cpf_cnpj_socio", ""))
            if len(socio_id) == 11:
                # Verificar se é servidor/candidato (PEP)
                servidores = self.db.buscar_servidores_cpf(socio_id)
                candidaturas = self.db.buscar_candidaturas(socio_id)
                if servidores or candidaturas:
                    pep_count += 1
                    evidencias.append(
                        f"{s.get('nome_socio', '')}: "
                        f"{'servidor' if servidores else ''}"
                        f"{'/' if servidores and candidaturas else ''}"
                        f"{'candidato' if candidaturas else ''}"
                    )

        score = min(1.0, pep_count * 0.4)
        return IndicadorRisco(
            nome="Sócio PEP (Pessoa Politicamente Exposta)",
            peso=peso,
            score=score,
            contribuicao=peso * score,
            detalhes=f"{pep_count} sócio(s) PEP" if pep_count else "Nenhum sócio PEP identificado",
            evidencias=evidencias[:5],
        )

    # ===================================================================
    # Resumo
    # ===================================================================

    def _gerar_resumo(self, resultado: ResultadoRisco) -> str:
        ativos = [i for i in resultado.indicadores if i.score > 0]
        if not ativos:
            return (
                f"Nenhum indicador de risco identificado para {resultado.nome}. "
                f"Score: {resultado.score_total:.1f}/100 (Nível: {resultado.nivel})."
            )

        top = sorted(ativos, key=lambda x: -x.contribuicao)[:3]
        parts = [f"- {i.nome}: {i.detalhes}" for i in top]
        return (
            f"Análise de risco para {resultado.nome}: Score {resultado.score_total:.1f}/100 "
            f"(Nível: {resultado.nivel}). "
            f"Principais indicadores:\n" + "\n".join(parts)
        )
