"""Módulo de análise avançada — centralidade, comunidades, clusters."""

from __future__ import annotations

from collections import Counter
from typing import Any

import networkx as nx
import pandas as pd

from horus.config import Config
from horus.database import DatabaseManager
from horus.graph_builder import EdgeType, GraphBuilder, NodeType
from horus.utils import get_logger

logger = get_logger(__name__)


class GraphAnalysis:
    """Análise de grafos: centralidade, comunidades, caminhos."""

    def __init__(self, graph_builder: GraphBuilder) -> None:
        self.gb = graph_builder

    @property
    def G(self) -> nx.DiGraph:
        return self.gb.graph

    # ------------------------------------------------------------------
    # Centralidade
    # ------------------------------------------------------------------

    def degree_centrality(self, top_n: int = 10) -> list[dict[str, Any]]:
        """Top-N nós por grau de centralidade."""
        if self.G.number_of_nodes() == 0:
            return []
        dc = nx.degree_centrality(self.G)
        top = sorted(dc.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [
            {
                "node_id": nid,
                "label": self.G.nodes[nid].get("label", nid),
                "tipo": self.G.nodes[nid].get("tipo", ""),
                "degree_centrality": round(val, 6),
            }
            for nid, val in top
        ]

    def betweenness_centrality(self, top_n: int = 10) -> list[dict[str, Any]]:
        """Top-N nós por betweenness centrality."""
        if self.G.number_of_nodes() < 3:
            return []
        bc = nx.betweenness_centrality(self.G)
        top = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [
            {
                "node_id": nid,
                "label": self.G.nodes[nid].get("label", nid),
                "tipo": self.G.nodes[nid].get("tipo", ""),
                "betweenness_centrality": round(val, 6),
            }
            for nid, val in top
        ]

    def pagerank(self, top_n: int = 10) -> list[dict[str, Any]]:
        """Top-N nós por PageRank."""
        if self.G.number_of_nodes() == 0:
            return []
        pr = nx.pagerank(self.G, max_iter=100)
        top = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [
            {
                "node_id": nid,
                "label": self.G.nodes[nid].get("label", nid),
                "tipo": self.G.nodes[nid].get("tipo", ""),
                "pagerank": round(val, 6),
            }
            for nid, val in top
        ]

    # ------------------------------------------------------------------
    # Comunidades
    # ------------------------------------------------------------------

    def detect_communities(self) -> list[set[str]]:
        """Detecta comunidades via Greedy Modularity (grafo não-dirigido)."""
        if self.G.number_of_nodes() < 2:
            return []
        undirected = self.G.to_undirected()
        try:
            communities = list(
                nx.community.greedy_modularity_communities(undirected)
            )
            return [set(c) for c in communities]
        except Exception as e:
            logger.warning("Erro detectando comunidades: %s", e)
            return []

    def community_summary(self) -> list[dict[str, Any]]:
        """Resumo das comunidades detectadas."""
        communities = self.detect_communities()
        summaries = []
        for i, comm in enumerate(communities):
            tipos = Counter(
                self.G.nodes[n].get("tipo", "unknown") for n in comm if n in self.G
            )
            summaries.append({
                "community_id": i,
                "size": len(comm),
                "tipos": dict(tipos),
                "membros_exemplo": list(comm)[:5],
            })
        return sorted(summaries, key=lambda x: -x["size"])

    # ------------------------------------------------------------------
    # Caminhos
    # ------------------------------------------------------------------

    def shortest_path(self, origem: str, destino: str) -> list[str] | None:
        """Caminho mais curto entre dois nós."""
        try:
            return nx.shortest_path(self.G, origem, destino)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def all_paths(self, origem: str, destino: str, max_depth: int = 5) -> list[list[str]]:
        """Todos os caminhos simples até profundidade máxima."""
        try:
            return list(nx.all_simple_paths(self.G, origem, destino, cutoff=max_depth))
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

    # ------------------------------------------------------------------
    # Subgrafos de interesse
    # ------------------------------------------------------------------

    def subgraph_by_type(self, node_type: str) -> nx.DiGraph:
        """Extrai subgrafo contendo apenas nós de um tipo."""
        nodes = [n for n, d in self.G.nodes(data=True) if d.get("tipo") == node_type]
        return self.G.subgraph(nodes).copy()

    def ego_graph(self, node_id: str, radius: int = 2) -> nx.DiGraph:
        """Grafo ego (vizinhança) de um nó."""
        try:
            return nx.ego_graph(self.G, node_id, radius=radius)
        except nx.NodeNotFound:
            return nx.DiGraph()

    # ------------------------------------------------------------------
    # Detecção de padrões suspeitos
    # ------------------------------------------------------------------

    def detect_triangles(self) -> list[dict[str, Any]]:
        """Detecta triângulos (ciclos de 3 nós) — possíveis esquemas circulares."""
        undirected = self.G.to_undirected()
        triangles: list[dict[str, Any]] = []

        for node in undirected.nodes():
            neighbors = set(undirected.neighbors(node))
            for n1 in neighbors:
                for n2 in neighbors:
                    if n1 < n2 and undirected.has_edge(n1, n2):
                        triangle = {
                            "nodes": sorted([node, n1, n2]),
                            "labels": [
                                self.G.nodes[n].get("label", n)
                                for n in sorted([node, n1, n2])
                            ],
                            "tipos": [
                                self.G.nodes[n].get("tipo", "")
                                for n in sorted([node, n1, n2])
                            ],
                        }
                        if triangle not in triangles:
                            triangles.append(triangle)

        return triangles[:50]  # Limitar

    def detect_hubs(self, min_connections: int = 5) -> list[dict[str, Any]]:
        """Detecta hubs — nós com muitas conexões."""
        hubs = []
        for nid, deg in self.G.degree():
            if deg >= min_connections:
                hubs.append({
                    "node_id": nid,
                    "label": self.G.nodes[nid].get("label", nid),
                    "tipo": self.G.nodes[nid].get("tipo", ""),
                    "connections": deg,
                })
        return sorted(hubs, key=lambda x: -x["connections"])

    def detect_bridges(self) -> list[tuple[str, str]]:
        """Detecta pontes (arestas cuja remoção desconecta o grafo)."""
        undirected = self.G.to_undirected()
        try:
            return list(nx.bridges(undirected))
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Resumo geral
    # ------------------------------------------------------------------

    def full_report(self) -> dict[str, Any]:
        """Gera relatório completo de análise do grafo."""
        return {
            "metricas": self.gb.metrics(),
            "top_degree": self.degree_centrality(5),
            "top_betweenness": self.betweenness_centrality(5),
            "top_pagerank": self.pagerank(5),
            "comunidades": self.community_summary(),
            "hubs": self.detect_hubs(3),
            "triangulos": len(self.detect_triangles()),
            "pontes": len(self.detect_bridges()),
        }
