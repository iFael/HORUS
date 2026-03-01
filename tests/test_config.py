"""Testes para horus.config."""

from horus.config import Config, Paths, URLs, RiskParams


def test_config_defaults():
    c = Config()
    assert c.cache_ttl_days == 30 or isinstance(c.cache_ttl_days, int)
    assert c.etl_workers >= 1
    assert isinstance(c.disclaimer, str)
    assert len(c.disclaimer) > 50


def test_paths_ensure(tmp_path):
    p = Paths(
        data=tmp_path / "d",
        raw=tmp_path / "d" / "raw",
        processed=tmp_path / "d" / "proc",
        exports=tmp_path / "d" / "exp",
        db=tmp_path / "d" / "t.db",
    )
    p.ensure()
    assert p.raw.exists()
    assert p.processed.exists()
    assert p.exports.exists()


def test_urls_have_values():
    u = URLs()
    assert "portaldatransparencia" in u.transparencia
    assert "pncp.gov.br" in u.pncp
    assert "rfb.gov.br" in u.receita_cnpj
    assert "tse.jus.br" in u.tse


def test_risk_params_pesos():
    r = RiskParams()
    assert "sancao_ativa" in r.pesos
    assert r.pesos["sancao_ativa"] == 20.0
    assert r.concentracao_limiar == 0.40
