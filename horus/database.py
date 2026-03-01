"""Camada de persistência SQLite do HORUS."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Generator

import pandas as pd

from horus.config import Config
from horus.utils import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Schema SQL
# ---------------------------------------------------------------------------

_SCHEMA = """
-- Pessoas físicas
CREATE TABLE IF NOT EXISTS pessoas (
    cpf             TEXT PRIMARY KEY,
    nome            TEXT NOT NULL,
    nome_normalizado TEXT,
    data_nascimento TEXT,
    uf              TEXT,
    municipio       TEXT,
    atualizado_em   TEXT
);

-- Empresas
CREATE TABLE IF NOT EXISTS empresas (
    cnpj            TEXT PRIMARY KEY,
    razao_social    TEXT,
    nome_fantasia   TEXT,
    data_abertura   TEXT,
    situacao        TEXT,
    natureza_juridica TEXT,
    porte           TEXT,
    endereco        TEXT,
    municipio       TEXT,
    uf              TEXT,
    cep             TEXT,
    cnae_principal  TEXT,
    capital_social  REAL,
    atualizado_em   TEXT
);

-- Sócios de empresas
CREATE TABLE IF NOT EXISTS socios (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cnpj            TEXT NOT NULL,
    tipo_socio      TEXT,          -- PF / PJ
    cpf_cnpj_socio  TEXT,
    nome_socio      TEXT,
    qualificacao    TEXT,
    data_entrada    TEXT,
    atualizado_em   TEXT,
    UNIQUE(cnpj, cpf_cnpj_socio)
);

-- Contratos públicos
CREATE TABLE IF NOT EXISTS contratos (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    numero          TEXT,
    orgao           TEXT,
    orgao_cnpj      TEXT,
    fornecedor_cnpj TEXT,
    fornecedor_nome TEXT,
    objeto          TEXT,
    valor           REAL,
    data_inicio     TEXT,
    data_fim        TEXT,
    modalidade      TEXT,
    fonte           TEXT,           -- transparencia / pncp
    atualizado_em   TEXT,
    UNIQUE(numero, orgao_cnpj, fonte)
);

-- Licitações
CREATE TABLE IF NOT EXISTS licitacoes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    numero          TEXT,
    orgao           TEXT,
    orgao_cnpj      TEXT,
    modalidade      TEXT,
    situacao        TEXT,
    objeto          TEXT,
    valor_estimado  REAL,
    data_abertura   TEXT,
    fonte           TEXT,
    atualizado_em   TEXT,
    UNIQUE(numero, orgao_cnpj, fonte)
);

-- Emendas parlamentares
CREATE TABLE IF NOT EXISTS emendas (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    numero          TEXT UNIQUE,
    autor           TEXT,
    autor_cpf       TEXT,
    tipo            TEXT,
    ano             INTEGER,
    valor_empenhado REAL,
    valor_pago      REAL,
    localidade      TEXT,
    uf              TEXT,
    funcao          TEXT,
    subfuncao       TEXT,
    atualizado_em   TEXT
);

-- Sanções (CEIS/CNEP/CEAF/CEPIM)
CREATE TABLE IF NOT EXISTS sancoes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo            TEXT NOT NULL,   -- CEIS/CNEP/CEAF/CEPIM
    cpf_cnpj        TEXT NOT NULL,
    nome            TEXT,
    orgao_sancionador TEXT,
    fundamentacao   TEXT,
    data_inicio     TEXT,
    data_fim        TEXT,
    uf              TEXT,
    fonte           TEXT,
    atualizado_em   TEXT,
    UNIQUE(tipo, cpf_cnpj, data_inicio)
);

-- Candidaturas TSE
CREATE TABLE IF NOT EXISTS candidaturas (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpf             TEXT,
    nome            TEXT,
    ano_eleicao     INTEGER,
    cargo           TEXT,
    partido         TEXT,
    uf              TEXT,
    municipio       TEXT,
    situacao        TEXT,
    total_bens      REAL,
    total_receitas  REAL,
    total_despesas  REAL,
    votos           INTEGER,
    atualizado_em   TEXT,
    UNIQUE(cpf, ano_eleicao, cargo)
);

-- Servidores públicos
CREATE TABLE IF NOT EXISTS servidores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpf             TEXT,
    nome            TEXT,
    orgao           TEXT,
    orgao_cnpj      TEXT,
    cargo           TEXT,
    funcao          TEXT,
    remuneracao     REAL,
    data_ingresso   TEXT,
    situacao_vinculo TEXT,
    uf              TEXT,
    atualizado_em   TEXT,
    UNIQUE(cpf, orgao_cnpj, cargo)
);

-- Relacionamentos genéricos (grafo)
CREATE TABLE IF NOT EXISTS relacionamentos (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    origem_tipo     TEXT NOT NULL,
    origem_id       TEXT NOT NULL,
    destino_tipo    TEXT NOT NULL,
    destino_id      TEXT NOT NULL,
    tipo_relacao    TEXT NOT NULL,
    peso            REAL DEFAULT 1.0,
    metadata_json   TEXT,
    atualizado_em   TEXT,
    UNIQUE(origem_tipo, origem_id, destino_tipo, destino_id, tipo_relacao)
);

-- Doações de campanha
CREATE TABLE IF NOT EXISTS doacoes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cpf_cnpj_doador TEXT,
    nome_doador     TEXT,
    cpf_candidato   TEXT,
    nome_candidato  TEXT,
    ano_eleicao     INTEGER,
    valor           REAL,
    tipo_recurso    TEXT,
    partido         TEXT,
    atualizado_em   TEXT,
    UNIQUE(cpf_cnpj_doador, cpf_candidato, ano_eleicao, valor)
);

-- Controle de cache de ETL
CREATE TABLE IF NOT EXISTS cache_metadata (
    chave           TEXT PRIMARY KEY,
    fonte           TEXT NOT NULL,
    atualizado_em   TEXT NOT NULL,
    expira_em       TEXT NOT NULL,
    registros       INTEGER DEFAULT 0,
    hash_dados      TEXT
);

-- Políticos (deputados + senadores)
CREATE TABLE IF NOT EXISTS politicos (
    id              TEXT PRIMARY KEY,
    id_externo      TEXT,
    cpf             TEXT,
    nome            TEXT NOT NULL,
    nome_civil      TEXT,
    partido         TEXT,
    uf              TEXT,
    cargo           TEXT,
    legislatura     INTEGER,
    foto_url        TEXT,
    email           TEXT,
    situacao        TEXT,
    atualizado_em   TEXT
);

-- Despesas parlamentares (Câmara / Senado)
CREATE TABLE IF NOT EXISTS despesas_parlamentares (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    politico_id     TEXT NOT NULL,
    ano             INTEGER,
    mes             INTEGER,
    tipo            TEXT,
    fornecedor_cnpj TEXT,
    fornecedor_nome TEXT,
    valor           REAL,
    valor_liquido   REAL,
    url_documento   TEXT,
    atualizado_em   TEXT,
    UNIQUE(politico_id, ano, mes, tipo, fornecedor_cnpj, valor)
);

-- Insights / Irregularidades detectadas
CREATE TABLE IF NOT EXISTS insights (
    id              TEXT PRIMARY KEY,
    politico_id     TEXT,
    politico_nome   TEXT,
    tipo            TEXT NOT NULL,
    titulo          TEXT NOT NULL,
    descricao       TEXT,
    severidade      TEXT NOT NULL,
    score           REAL,
    valor_exposicao REAL DEFAULT 0,
    pattern         TEXT,
    fontes          TEXT,
    dados_json      TEXT,
    criado_em       TEXT,
    atualizado_em   TEXT
);

-- Varreduras (scan runs)
CREATE TABLE IF NOT EXISTS varreduras (
    id              TEXT PRIMARY KEY,
    inicio          TEXT,
    fim             TEXT,
    status          TEXT,
    total_politicos INTEGER DEFAULT 0,
    total_insights  INTEGER DEFAULT 0,
    total_alertas   INTEGER DEFAULT 0,
    log_resumo      TEXT
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj);
CREATE INDEX IF NOT EXISTS idx_socios_cpf_socio ON socios(cpf_cnpj_socio);
CREATE INDEX IF NOT EXISTS idx_contratos_fornecedor ON contratos(fornecedor_cnpj);
CREATE INDEX IF NOT EXISTS idx_contratos_orgao ON contratos(orgao_cnpj);
CREATE INDEX IF NOT EXISTS idx_sancoes_cpf_cnpj ON sancoes(cpf_cnpj);
CREATE INDEX IF NOT EXISTS idx_candidaturas_cpf ON candidaturas(cpf);
CREATE INDEX IF NOT EXISTS idx_servidores_cpf ON servidores(cpf);
CREATE INDEX IF NOT EXISTS idx_emendas_autor_cpf ON emendas(autor_cpf);
CREATE INDEX IF NOT EXISTS idx_doacoes_doador ON doacoes(cpf_cnpj_doador);
CREATE INDEX IF NOT EXISTS idx_doacoes_candidato ON doacoes(cpf_candidato);
CREATE INDEX IF NOT EXISTS idx_rel_origem ON relacionamentos(origem_tipo, origem_id);
CREATE INDEX IF NOT EXISTS idx_rel_destino ON relacionamentos(destino_tipo, destino_id);
CREATE INDEX IF NOT EXISTS idx_pessoas_nome ON pessoas(nome_normalizado);
CREATE INDEX IF NOT EXISTS idx_empresas_uf ON empresas(uf);
CREATE INDEX IF NOT EXISTS idx_politicos_nome ON politicos(nome);
CREATE INDEX IF NOT EXISTS idx_politicos_uf ON politicos(uf);
CREATE INDEX IF NOT EXISTS idx_politicos_partido ON politicos(partido);
CREATE INDEX IF NOT EXISTS idx_despesas_politico ON despesas_parlamentares(politico_id);
CREATE INDEX IF NOT EXISTS idx_despesas_fornecedor ON despesas_parlamentares(fornecedor_cnpj);
CREATE INDEX IF NOT EXISTS idx_insights_politico ON insights(politico_id);
CREATE INDEX IF NOT EXISTS idx_insights_severidade ON insights(severidade);
CREATE INDEX IF NOT EXISTS idx_insights_tipo ON insights(tipo);
"""


class DatabaseManager:
    """Gerencia conexão SQLite e operações CRUD."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self.db_path: Path = self.config.paths.db
        self._init_db()

    # ------------------------------------------------------------------
    # Conexão
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(_SCHEMA)
        logger.info("Banco de dados inicializado: %s", self.db_path)

    @contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-4000")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Upsert genérico
    # ------------------------------------------------------------------

    def upsert_df(self, table: str, df: pd.DataFrame) -> int:
        """Insere ou atualiza DataFrame na tabela. Retorna linhas afetadas."""
        if df.empty:
            return 0
        cols = list(df.columns)
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(cols)
        sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
        rows = df.where(df.notna(), None).values.tolist()
        with self.connect() as conn:
            conn.executemany(sql, rows)
            count = len(rows)
        logger.info("Upsert %s: %d registros", table, count)
        return count

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def cache_valido(self, chave: str) -> bool:
        """Verifica se o cache para a chave ainda é válido."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT expira_em FROM cache_metadata WHERE chave = ?", (chave,)
            ).fetchone()
        if not row:
            return False
        return datetime.fromisoformat(row["expira_em"]) > datetime.now()

    def atualizar_cache(
        self, chave: str, fonte: str, registros: int, hash_dados: str = ""
    ) -> None:
        """Registra atualização de cache."""
        agora = datetime.now()
        expira = agora + timedelta(days=self.config.cache_ttl_days)
        with self.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO cache_metadata
                   (chave, fonte, atualizado_em, expira_em, registros, hash_dados)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (chave, fonte, agora.isoformat(), expira.isoformat(), registros, hash_dados),
            )

    # ------------------------------------------------------------------
    # Queries comuns
    # ------------------------------------------------------------------

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Executa query e retorna lista de dicts."""
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def query_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Executa query e retorna DataFrame."""
        with self.connect() as conn:
            return pd.read_sql_query(sql, conn, params=params)

    def buscar_pessoa_cpf(self, cpf: str) -> dict[str, Any] | None:
        rows = self.query("SELECT * FROM pessoas WHERE cpf = ?", (cpf,))
        return rows[0] if rows else None

    def buscar_empresa_cnpj(self, cnpj: str) -> dict[str, Any] | None:
        rows = self.query("SELECT * FROM empresas WHERE cnpj = ?", (cnpj,))
        return rows[0] if rows else None

    def buscar_sancoes(self, cpf_cnpj: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM sancoes WHERE cpf_cnpj = ?", (cpf_cnpj,))

    def buscar_contratos_fornecedor(self, cnpj: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM contratos WHERE fornecedor_cnpj = ?", (cnpj,))

    def buscar_socios_empresa(self, cnpj: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM socios WHERE cnpj = ?", (cnpj,))

    def buscar_empresas_socio(self, cpf_cnpj: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM socios WHERE cpf_cnpj_socio = ?", (cpf_cnpj,))

    def buscar_candidaturas(self, cpf: str) -> list[dict[str, Any]]:
        return self.query(
            "SELECT * FROM candidaturas WHERE cpf = ? ORDER BY ano_eleicao DESC", (cpf,)
        )

    def buscar_servidores_cpf(self, cpf: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM servidores WHERE cpf = ?", (cpf,))

    def buscar_emendas_autor(self, cpf: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM emendas WHERE autor_cpf = ?", (cpf,))

    def buscar_doacoes_doador(self, cpf_cnpj: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM doacoes WHERE cpf_cnpj_doador = ?", (cpf_cnpj,))

    def buscar_doacoes_candidato(self, cpf: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM doacoes WHERE cpf_candidato = ?", (cpf,))

    def buscar_pessoa_nome(self, nome: str, limite: int = 20) -> list[dict[str, Any]]:
        """Busca por nome normalizado (LIKE)."""
        from horus.utils import normalizar_nome
        norm = normalizar_nome(nome)
        return self.query(
            "SELECT * FROM pessoas WHERE nome_normalizado LIKE ? LIMIT ?",
            (f"%{norm}%", limite),
        )

    def contratos_por_orgao(self, orgao_cnpj: str) -> pd.DataFrame:
        return self.query_df(
            "SELECT * FROM contratos WHERE orgao_cnpj = ?", (orgao_cnpj,)
        )

    def estatisticas(self) -> dict[str, int]:
        """Retorna contagem de registros por tabela."""
        tabelas = [
            "pessoas", "empresas", "socios", "contratos", "licitacoes",
            "emendas", "sancoes", "candidaturas", "servidores",
            "relacionamentos", "doacoes", "politicos",
            "despesas_parlamentares", "insights", "varreduras",
        ]
        stats: dict[str, int] = {}
        with self.connect() as conn:
            for t in tabelas:
                row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {t}").fetchone()
                stats[t] = row["cnt"] if row else 0
        return stats

    # ------------------------------------------------------------------
    # Políticos
    # ------------------------------------------------------------------

    def buscar_politicos(self, cargo: str = "", uf: str = "", partido: str = "",
                         nome: str = "", limite: int = 1000) -> list[dict[str, Any]]:
        sql = "SELECT * FROM politicos WHERE 1=1"
        params: list[Any] = []
        if nome:
            sql += " AND nome LIKE ?"
            params.append(f"%{nome}%")
        if cargo:
            sql += " AND cargo = ?"
            params.append(cargo)
        if uf:
            sql += " AND uf = ?"
            params.append(uf)
        if partido:
            sql += " AND partido = ?"
            params.append(partido)
        sql += " ORDER BY nome LIMIT ?"
        params.append(limite)
        return self.query(sql, tuple(params))

    def buscar_politico_id(self, politico_id: str) -> dict[str, Any] | None:
        rows = self.query("SELECT * FROM politicos WHERE id = ?", (politico_id,))
        return rows[0] if rows else None

    # ------------------------------------------------------------------
    # Insights
    # ------------------------------------------------------------------

    def buscar_insights(self, politico_id: str = "", severidade: str = "",
                        tipo: str = "", limite: int = 500) -> list[dict[str, Any]]:
        sql = "SELECT * FROM insights WHERE 1=1"
        params: list[Any] = []
        if politico_id:
            sql += " AND politico_id = ?"
            params.append(politico_id)
        if severidade:
            sql += " AND severidade = ?"
            params.append(severidade)
        if tipo:
            sql += " AND tipo = ?"
            params.append(tipo)
        sql += " ORDER BY score DESC LIMIT ?"
        params.append(limite)
        return self.query(sql, tuple(params))

    def buscar_despesas_politico(self, politico_id: str, ano: int = 0) -> pd.DataFrame:
        sql = "SELECT * FROM despesas_parlamentares WHERE politico_id = ?"
        params: list[Any] = [politico_id]
        if ano:
            sql += " AND ano = ?"
            params.append(ano)
        sql += " ORDER BY ano DESC, mes DESC"
        return self.query_df(sql, tuple(params))

    def get_dashboard_stats(self) -> dict[str, Any]:
        """Retorna estatísticas para o dashboard."""
        with self.connect() as conn:
            politicos = conn.execute("SELECT COUNT(*) AS c FROM politicos").fetchone()["c"]
            insights_total = conn.execute("SELECT COUNT(*) AS c FROM insights").fetchone()["c"]
            alertas = conn.execute(
                "SELECT COUNT(*) AS c FROM insights WHERE severidade IN ('CRITICO', 'ALTO')"
            ).fetchone()["c"]
            exposicao = conn.execute(
                "SELECT COALESCE(SUM(valor_exposicao), 0) AS s FROM insights"
            ).fetchone()["s"]
            fontes = conn.execute(
                "SELECT COUNT(DISTINCT fontes) AS c FROM insights WHERE fontes IS NOT NULL"
            ).fetchone()["c"]
            ultima_varredura = conn.execute(
                "SELECT * FROM varreduras ORDER BY inicio DESC LIMIT 1"
            ).fetchone()
        return {
            "politicos": politicos,
            "insights": insights_total,
            "alertas": alertas,
            "exposicao_total": exposicao,
            "fontes": fontes,
            "ultima_varredura": dict(ultima_varredura) if ultima_varredura else None,
        }
