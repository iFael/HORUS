"""Testes para raiox.utils."""

from raiox.utils import (
    limpar_documento,
    validar_cpf,
    validar_cnpj,
    formatar_cpf,
    formatar_cnpj,
    normalizar_nome,
    similaridade_nomes,
    mesmo_sobrenome,
    hash_params,
    RateLimiter,
)


class TestLimparDocumento:
    def test_remove_pontuacao(self):
        assert limpar_documento("123.456.789-00") == "12345678900"

    def test_remove_barra(self):
        assert limpar_documento("12.345.678/0001-00") == "12345678000100"

    def test_vazio(self):
        assert limpar_documento("") == ""

    def test_apenas_numeros(self):
        assert limpar_documento("12345678900") == "12345678900"


class TestValidarCPF:
    def test_cpf_valido(self):
        assert validar_cpf("52998224725") is True

    def test_cpf_invalido(self):
        assert validar_cpf("12345678900") is False

    def test_cpf_repeticao(self):
        assert validar_cpf("11111111111") is False

    def test_cpf_formatado(self):
        assert validar_cpf("529.982.247-25") is True

    def test_cpf_curto(self):
        assert validar_cpf("123") is False


class TestValidarCNPJ:
    def test_cnpj_valido(self):
        assert validar_cnpj("11222333000181") is True

    def test_cnpj_invalido(self):
        assert validar_cnpj("12345678000100") is False

    def test_cnpj_repeticao(self):
        assert validar_cnpj("11111111111111") is False


class TestFormatar:
    def test_formatar_cpf(self):
        assert formatar_cpf("52998224725") == "529.982.247-25"

    def test_formatar_cnpj(self):
        assert formatar_cnpj("11222333000181") == "11.222.333/0001-81"


class TestNormalizarNome:
    def test_acentos(self):
        assert normalizar_nome("José da Sílva") == "JOSE DA SILVA"

    def test_espacos(self):
        assert normalizar_nome("  João   Maria  ") == "JOAO MARIA"

    def test_vazio(self):
        assert normalizar_nome("") == ""


class TestSimilaridadeNomes:
    def test_iguais(self):
        assert similaridade_nomes("João Silva", "JOAO SILVA") == 100

    def test_parecidos(self):
        score = similaridade_nomes("João da Silva", "Joao Silva")
        assert score >= 70

    def test_diferentes(self):
        score = similaridade_nomes("João Silva", "Pedro Santos")
        assert score < 50


class TestMesmoSobrenome:
    def test_mesmo(self):
        assert mesmo_sobrenome("João da Silva", "Maria da Silva") is True

    def test_diferente(self):
        assert mesmo_sobrenome("João Silva", "Pedro Santos") is False

    def test_nome_simples(self):
        assert mesmo_sobrenome("João", "Maria") is False


class TestHashParams:
    def test_deterministico(self):
        h1 = hash_params(a="1", b="2")
        h2 = hash_params(a="1", b="2")
        assert h1 == h2

    def test_ordem_nao_importa(self):
        h1 = hash_params(a="1", b="2")
        h2 = hash_params(b="2", a="1")
        assert h1 == h2

    def test_valores_diferentes(self):
        h1 = hash_params(a="1")
        h2 = hash_params(a="2")
        assert h1 != h2


class TestRateLimiter:
    def test_nao_bloqueia_primeiro(self):
        rl = RateLimiter()
        import time
        start = time.time()
        rl.wait("test", max_per_minute=100)
        elapsed = time.time() - start
        assert elapsed < 1.0
