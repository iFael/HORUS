"""Testes para horus.analysis."""

import pytest

from horus.analysis import GraphAnalysis
from horus.graph_builder import GraphBuilder, NodeType, EdgeType


class TestGraphAnalysis:
    def _make_graph(self, populated_db) -> GraphAnalysis:
        gb = GraphBuilder(populated_db)
        gb.build_from_cpf("12345678900", profundidade=2)
        gb.add_same_address_edges()
        gb.add_family_edges()
        return GraphAnalysis(gb)

    def test_degree_centrality(self, populated_db):
        ga = self._make_graph(populated_db)
        result = ga.degree_centrality(5)
        if ga.G.number_of_nodes() > 0:
            assert len(result) > 0
            assert "node_id" in result[0]
            assert "degree_centrality" in result[0]

    def test_betweenness_centrality(self, populated_db):
        ga = self._make_graph(populated_db)
        result = ga.betweenness_centrality(5)
        # Pode estar vazio se grafo pequeno
        assert isinstance(result, list)

    def test_pagerank(self, populated_db):
        ga = self._make_graph(populated_db)
        result = ga.pagerank(5)
        if ga.G.number_of_nodes() > 0:
            assert len(result) > 0

    def test_detect_communities(self, populated_db):
        ga = self._make_graph(populated_db)
        comms = ga.detect_communities()
        assert isinstance(comms, list)

    def test_community_summary(self, populated_db):
        ga = self._make_graph(populated_db)
        summary = ga.community_summary()
        assert isinstance(summary, list)

    def test_detect_hubs(self, populated_db):
        ga = self._make_graph(populated_db)
        hubs = ga.detect_hubs(min_connections=1)
        assert isinstance(hubs, list)

    def test_ego_graph(self, populated_db):
        ga = self._make_graph(populated_db)
        ego = ga.ego_graph("pessoa:12345678900", radius=1)
        assert ego.number_of_nodes() >= 1

    def test_ego_graph_not_found(self, populated_db):
        ga = self._make_graph(populated_db)
        ego = ga.ego_graph("nao_existe", radius=1)
        assert ego.number_of_nodes() == 0

    def test_full_report(self, populated_db):
        ga = self._make_graph(populated_db)
        report = ga.full_report()
        assert "metricas" in report
        assert "top_degree" in report
        assert "comunidades" in report
        assert "hubs" in report

    def test_empty_graph(self, db):
        gb = GraphBuilder(db)
        ga = GraphAnalysis(gb)
        report = ga.full_report()
        assert report["metricas"]["nodes"] == 0

    def test_detect_bridges(self, populated_db):
        ga = self._make_graph(populated_db)
        bridges = ga.detect_bridges()
        assert isinstance(bridges, list)

    def test_subgraph_by_type(self, populated_db):
        ga = self._make_graph(populated_db)
        sub = ga.subgraph_by_type(NodeType.PESSOA)
        for _, data in sub.nodes(data=True):
            assert data.get("tipo") == NodeType.PESSOA
