"""Testes para horus.graph_builder."""

import pytest

from horus.graph_builder import GraphBuilder, NodeType, EdgeType


class TestGraphBuilder:
    def test_add_node(self, db):
        gb = GraphBuilder(db)
        nid = gb.add_node(NodeType.PESSOA, "12345678900", nome="Test")
        assert nid == "pessoa:12345678900"
        assert gb.graph.number_of_nodes() == 1

    def test_add_edge(self, db):
        gb = GraphBuilder(db)
        n1 = gb.add_node(NodeType.PESSOA, "111", nome="A")
        n2 = gb.add_node(NodeType.EMPRESA, "222", nome="B")
        gb.add_edge(n1, n2, EdgeType.SOCIO_DE)
        assert gb.graph.number_of_edges() == 1
        edge_data = gb.graph.edges[n1, n2]
        assert edge_data["tipo"] == EdgeType.SOCIO_DE

    def test_build_from_cpf(self, populated_db):
        gb = GraphBuilder(populated_db)
        G = gb.build_from_cpf("12345678900", profundidade=1)
        assert G.number_of_nodes() > 0
        # Deve conter o nó da pessoa
        assert "pessoa:12345678900" in G

    def test_build_from_cnpj(self, populated_db):
        gb = GraphBuilder(populated_db)
        G = gb.build_from_cnpj("11111111000100", profundidade=1)
        assert G.number_of_nodes() > 0
        assert "empresa:11111111000100" in G

    def test_same_address_edges(self, populated_db):
        gb = GraphBuilder(populated_db)
        # Duas empresas no mesmo endereço
        gb.build_from_cnpj("11111111000100", profundidade=1)
        # Adicionar segunda empresa manualmente se não no grafo
        gb.add_node(NodeType.EMPRESA, "22222222000100",
                     nome="CONSTRUTORA XYZ SA", endereco="RUA A 100 CENTRO")
        gb.graph.nodes["empresa:11111111000100"]["endereco"] = "RUA A 100 CENTRO"
        count = gb.add_same_address_edges()
        assert count >= 1

    def test_family_edges(self, populated_db):
        gb = GraphBuilder(populated_db)
        gb.add_node(NodeType.PESSOA, "12345678900", nome="JOAO DA SILVA")
        gb.add_node(NodeType.PESSOA, "98765432100", nome="MARIA DA SILVA")
        count = gb.add_family_edges()
        assert count >= 1

    def test_metrics(self, populated_db):
        gb = GraphBuilder(populated_db)
        gb.build_from_cpf("12345678900", profundidade=1)
        m = gb.metrics()
        assert "nodes" in m
        assert "edges" in m
        assert m["nodes"] > 0

    def test_to_dict(self, populated_db):
        gb = GraphBuilder(populated_db)
        gb.build_from_cpf("12345678900", profundidade=1)
        d = gb.to_dict()
        assert "nodes" in d
        assert "edges" in d or "links" in d

    def test_empty_graph(self, db):
        gb = GraphBuilder(db)
        m = gb.metrics()
        assert m["nodes"] == 0
        assert m["edges"] == 0
