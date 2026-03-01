"""Construtor de Grafo de Conhecimento — NetworkX DiGraph."""

from __future__ import annotations

from typing import Any

import networkx as nx
import pandas as pd

from horus.config import Config
from horus.database import DatabaseManager
from horus.utils import get_logger, limpar_documento, normalizar_nome

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Tipos de nó e aresta
# ---------------------------------------------------------------------------

class NodeType:
    PESSOA = "pessoa"
    EMPRESA = "empresa"
    ORGAO = "orgao"
    CONTRATO = "contrato"
    LICITACAO = "licitacao"
    EMENDA = "emenda"
    SANCAO = "sancao"
    CANDIDATURA = "candidatura"


class EdgeType:
    SOCIO_DE = "socio_de"
    CONTRATADO_POR = "contratado_por"
    SERVIDOR_EM = "servidor_em"
    SANCIONADO = "sancionado"
    CANDIDATO_A = "candidato_a"
    AUTOR_EMENDA = "autor_emenda"
    BENEFICIARIO_EMENDA = "beneficiario_emenda"
    DOOU_PARA = "doou_para"
    RECEBEU_DOACAO = "recebeu_doacao"
    FORNECEU_PARA = "forneceu_para"
    MESMO_ENDERECO = "mesmo_endereco"
    PARENTE_DE = "parente_de"
    LICITA_EM = "licita_em"


class GraphBuilder:
    """Constrói e mantém o grafo de conhecimento a partir do banco de dados."""

    def __init__(self, db: DatabaseManager, config: Config | None = None) -> None:
        self.db = db
        self.config = config or db.config
        self.graph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Helpers de nó
    # ------------------------------------------------------------------

    def _node_id(self, tipo: str, identificador: str) -> str:
        return f"{tipo}:{identificador}"

    def add_node(self, tipo: str, identificador: str, **attrs: Any) -> str:
        nid = self._node_id(tipo, identificador)
        self.graph.add_node(nid, tipo=tipo, identificador=identificador, **attrs)
        return nid

    def add_edge(self, origem: str, destino: str, tipo_relacao: str, **attrs: Any) -> None:
        self.graph.add_edge(origem, destino, tipo=tipo_relacao, **attrs)

    # ------------------------------------------------------------------
    # Construção do grafo por CPF
    # ------------------------------------------------------------------

    def build_from_cpf(self, cpf: str, profundidade: int = 2) -> nx.DiGraph:
        """Constrói subgrafo centrado em um CPF."""
        cpf = limpar_documento(cpf)
        self.graph.clear()
        self._expand_pessoa(cpf, depth=0, max_depth=profundidade)
        logger.info(
            "Grafo construído: %d nós, %d arestas",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )
        return self.graph

    def build_from_cnpj(self, cnpj: str, profundidade: int = 2) -> nx.DiGraph:
        """Constrói subgrafo centrado em um CNPJ."""
        cnpj = limpar_documento(cnpj)
        self.graph.clear()
        self._expand_empresa(cnpj, depth=0, max_depth=profundidade)
        logger.info(
            "Grafo construído: %d nós, %d arestas",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )
        return self.graph

    def build_from_nome(self, nome: str, profundidade: int = 2) -> nx.DiGraph:
        """Busca pessoa por nome e constrói grafos para todas encontradas."""
        self.graph.clear()
        pessoas = self.db.buscar_pessoa_nome(nome, limite=5)
        for p in pessoas:
            self._expand_pessoa(p["cpf"], depth=0, max_depth=profundidade)
        return self.graph

    # ------------------------------------------------------------------
    # Expansão recursiva
    # ------------------------------------------------------------------

    def _expand_pessoa(self, cpf: str, depth: int, max_depth: int) -> None:
        nid = self._node_id(NodeType.PESSOA, cpf)
        if nid in self.graph:
            return
        if depth > max_depth:
            return

        # Buscar dados da pessoa
        pessoa = self.db.buscar_pessoa_cpf(cpf)
        nome = pessoa["nome"] if pessoa else ""
        self.add_node(NodeType.PESSOA, cpf, nome=nome, label=nome or cpf)

        # Servidores
        servidores = self.db.buscar_servidores_cpf(cpf)
        for s in servidores:
            orgao_id = s.get("orgao_cnpj") or s.get("orgao", "desconhecido")
            oid = self.add_node(
                NodeType.ORGAO, orgao_id,
                nome=s.get("orgao", ""), label=s.get("orgao", orgao_id)
            )
            self.add_edge(nid, oid, EdgeType.SERVIDOR_EM,
                          cargo=s.get("cargo", ""),
                          remuneracao=s.get("remuneracao", 0))

        # Sanções
        sancoes = self.db.buscar_sancoes(cpf)
        for san in sancoes:
            sid = self.add_node(
                NodeType.SANCAO, f"{san['tipo']}_{san.get('data_inicio', '')}_{cpf}",
                tipo=san["tipo"], label=f"Sanção {san['tipo']}"
            )
            self.add_edge(nid, sid, EdgeType.SANCIONADO,
                          tipo_sancao=san["tipo"],
                          data_inicio=san.get("data_inicio", ""))

        # Candidaturas
        candidaturas = self.db.buscar_candidaturas(cpf)
        for cand in candidaturas:
            cid = self.add_node(
                NodeType.CANDIDATURA,
                f"{cpf}_{cand.get('ano_eleicao', '')}_{cand.get('cargo', '')}",
                label=f"{cand.get('cargo', '')} {cand.get('ano_eleicao', '')}"
            )
            self.add_edge(nid, cid, EdgeType.CANDIDATO_A,
                          ano=cand.get("ano_eleicao", ""),
                          partido=cand.get("partido", ""))

        # Emendas
        emendas = self.db.buscar_emendas_autor(cpf)
        for em in emendas:
            eid = self.add_node(
                NodeType.EMENDA, em.get("numero", ""),
                label=f"Emenda {em.get('numero', '')}",
                valor=em.get("valor_empenhado", 0)
            )
            self.add_edge(nid, eid, EdgeType.AUTOR_EMENDA,
                          valor=em.get("valor_empenhado", 0))

        # Doações recebidas
        doacoes = self.db.buscar_doacoes_candidato(cpf)
        for d in doacoes:
            doador_id = d.get("cpf_cnpj_doador", "")
            if doador_id:
                did = self.add_node(
                    NodeType.EMPRESA if len(limpar_documento(doador_id)) == 14 else NodeType.PESSOA,
                    limpar_documento(doador_id),
                    nome=d.get("nome_doador", ""),
                    label=d.get("nome_doador", doador_id)
                )
                self.add_edge(did, nid, EdgeType.DOOU_PARA,
                              valor=d.get("valor", 0),
                              ano=d.get("ano_eleicao", ""))

        # Empresas como sócio
        empresas = self.db.buscar_empresas_socio(cpf)
        for emp in empresas:
            cnpj = emp.get("cnpj", "")
            if cnpj and depth < max_depth:
                self._expand_empresa(cnpj, depth + 1, max_depth)
                enid = self._node_id(NodeType.EMPRESA, cnpj)
                self.add_edge(nid, enid, EdgeType.SOCIO_DE,
                              qualificacao=emp.get("qualificacao", ""))

    def _expand_empresa(self, cnpj: str, depth: int, max_depth: int) -> None:
        nid = self._node_id(NodeType.EMPRESA, cnpj)
        if nid in self.graph:
            return
        if depth > max_depth:
            return

        empresa = self.db.buscar_empresa_cnpj(cnpj)
        nome = empresa["razao_social"] if empresa else ""
        self.add_node(
            NodeType.EMPRESA, cnpj,
            nome=nome, label=nome or cnpj,
            endereco=empresa.get("endereco", "") if empresa else ""
        )

        # Sócios
        socios = self.db.buscar_socios_empresa(cnpj)
        for s in socios:
            socio_id = limpar_documento(s.get("cpf_cnpj_socio", ""))
            if not socio_id:
                continue
            if len(socio_id) == 11 and depth < max_depth:
                self._expand_pessoa(socio_id, depth + 1, max_depth)
            elif len(socio_id) == 14 and depth < max_depth:
                self._expand_empresa(socio_id, depth + 1, max_depth)
            else:
                tipo = NodeType.PESSOA if len(socio_id) == 11 else NodeType.EMPRESA
                self.add_node(tipo, socio_id,
                              nome=s.get("nome_socio", ""),
                              label=s.get("nome_socio", socio_id))

            tipo = NodeType.PESSOA if len(socio_id) == 11 else NodeType.EMPRESA
            sid = self._node_id(tipo, socio_id)
            self.add_edge(sid, nid, EdgeType.SOCIO_DE,
                          qualificacao=s.get("qualificacao", ""))

        # Contratos como fornecedor
        contratos = self.db.buscar_contratos_fornecedor(cnpj)
        for c in contratos:
            cid = self.add_node(
                NodeType.CONTRATO, c.get("numero", str(c.get("id", ""))),
                label=f"Contrato {c.get('numero', '')}",
                valor=c.get("valor", 0),
                objeto=c.get("objeto", ""),
            )
            self.add_edge(nid, cid, EdgeType.FORNECEU_PARA,
                          valor=c.get("valor", 0))

            orgao_cnpj = c.get("orgao_cnpj", "")
            if orgao_cnpj:
                oid = self.add_node(
                    NodeType.ORGAO, orgao_cnpj,
                    nome=c.get("orgao", ""), label=c.get("orgao", orgao_cnpj)
                )
                self.add_edge(cid, oid, EdgeType.CONTRATADO_POR)

        # Sanções
        sancoes = self.db.buscar_sancoes(cnpj)
        for san in sancoes:
            sid = self.add_node(
                NodeType.SANCAO, f"{san['tipo']}_{san.get('data_inicio', '')}_{cnpj}",
                tipo_sancao=san["tipo"], label=f"Sanção {san['tipo']}"
            )
            self.add_edge(nid, sid, EdgeType.SANCIONADO)

    # ------------------------------------------------------------------
    # Detecção de empresas com mesmo endereço
    # ------------------------------------------------------------------

    def add_same_address_edges(self) -> int:
        """Adiciona arestas entre empresas no mesmo endereço."""
        empresas = [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("tipo") == NodeType.EMPRESA and data.get("endereco")
        ]

        count = 0
        enderecos: dict[str, list[str]] = {}
        for nid, data in empresas:
            end = normalizar_nome(data["endereco"])
            if end:
                enderecos.setdefault(end, []).append(nid)

        for end, nodes in enderecos.items():
            if len(nodes) >= 2:
                for i, n1 in enumerate(nodes):
                    for n2 in nodes[i + 1:]:
                        self.add_edge(n1, n2, EdgeType.MESMO_ENDERECO)
                        self.add_edge(n2, n1, EdgeType.MESMO_ENDERECO)
                        count += 1

        return count

    # ------------------------------------------------------------------
    # Detecção de parentesco por sobrenome
    # ------------------------------------------------------------------

    def add_family_edges(self) -> int:
        """Adiciona arestas PARENTE_DE entre pessoas com mesmo sobrenome."""
        from horus.utils import mesmo_sobrenome

        pessoas = [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("tipo") == NodeType.PESSOA and data.get("nome")
        ]

        count = 0
        for i, (nid1, data1) in enumerate(pessoas):
            for nid2, data2 in pessoas[i + 1:]:
                if mesmo_sobrenome(data1["nome"], data2["nome"]):
                    self.add_edge(nid1, nid2, EdgeType.PARENTE_DE)
                    self.add_edge(nid2, nid1, EdgeType.PARENTE_DE)
                    count += 1

        return count

    # ------------------------------------------------------------------
    # Exportação
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Exporta grafo como dict serializável."""
        return nx.node_link_data(self.graph)

    def to_pyvis_html(self, output_path: str = "grafo.html", height: str = "800px") -> str:
        """Gera visualização interativa com PyVis."""
        from pyvis.network import Network

        net = Network(
            height=height, width="100%", directed=True,
            bgcolor="#1a1a1a", font_color="white"
        )

        color_map = {
            NodeType.PESSOA: "#4FC3F7",
            NodeType.EMPRESA: "#81C784",
            NodeType.ORGAO: "#FFB74D",
            NodeType.CONTRATO: "#E57373",
            NodeType.LICITACAO: "#BA68C8",
            NodeType.EMENDA: "#FFF176",
            NodeType.SANCAO: "#EF5350",
            NodeType.CANDIDATURA: "#64B5F6",
        }

        for nid, data in self.graph.nodes(data=True):
            tipo = data.get("tipo", "")
            net.add_node(
                nid,
                label=data.get("label", nid)[:30],
                color=color_map.get(tipo, "#90A4AE"),
                title=f"{tipo}: {data.get('nome', nid)}",
                size=20,
            )

        for u, v, data in self.graph.edges(data=True):
            net.add_edge(u, v, title=data.get("tipo", ""), label=data.get("tipo", "")[:15])

        net.set_options("""
        {
            "physics": {
                "barnesHut": {"gravitationalConstant": -8000, "springLength": 200},
                "stabilization": {"iterations": 150}
            },
            "interaction": {"hover": true, "tooltipDelay": 100}
        }
        """)

        net.save_graph(output_path)
        return output_path

    # ------------------------------------------------------------------
    # Métricas do grafo
    # ------------------------------------------------------------------

    def metrics(self) -> dict[str, Any]:
        """Calcula métricas básicas do grafo."""
        G = self.graph
        if G.number_of_nodes() == 0:
            return {"nodes": 0, "edges": 0}

        undirected = G.to_undirected()
        components = list(nx.connected_components(undirected))

        result: dict[str, Any] = {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "density": nx.density(G),
            "components": len(components),
            "largest_component": max(len(c) for c in components) if components else 0,
        }

        if G.number_of_nodes() > 1:
            try:
                dc = nx.degree_centrality(G)
                top_dc = sorted(dc.items(), key=lambda x: x[1], reverse=True)[:5]
                result["top_degree_centrality"] = [
                    {"node": n, "centrality": round(c, 4)} for n, c in top_dc
                ]
            except Exception:
                pass

            try:
                bc = nx.betweenness_centrality(G)
                top_bc = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:5]
                result["top_betweenness_centrality"] = [
                    {"node": n, "centrality": round(c, 4)} for n, c in top_bc
                ]
            except Exception:
                pass

        return result
