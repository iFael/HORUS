"""Funções utilitárias do RaioX Público BR."""

from __future__ import annotations

import hashlib
import logging
import re
import time
import unicodedata
from collections import defaultdict
from functools import wraps
from typing import Any, Callable

from thefuzz import fuzz

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Cria logger padronizado."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


# ---------------------------------------------------------------------------
# Validação de CPF / CNPJ
# ---------------------------------------------------------------------------

def limpar_documento(doc: str) -> str:
    """Remove pontuação e espaços de CPF/CNPJ."""
    return re.sub(r"[^0-9]", "", str(doc).strip())


def validar_cpf(cpf: str) -> bool:
    """Valida CPF (11 dígitos, cálculo dos verificadores)."""
    cpf = limpar_documento(cpf)
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    for i in range(9, 11):
        total = sum(int(cpf[j]) * ((i + 1) - j) for j in range(i))
        digito = (total * 10 % 11) % 10
        if int(cpf[i]) != digito:
            return False
    return True


def validar_cnpj(cnpj: str) -> bool:
    """Valida CNPJ (14 dígitos, cálculo dos verificadores)."""
    cnpj = limpar_documento(cnpj)
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(cnpj[i]) * pesos1[i] for i in range(12))
    d1 = 0 if soma1 % 11 < 2 else 11 - soma1 % 11
    if int(cnpj[12]) != d1:
        return False
    soma2 = sum(int(cnpj[i]) * pesos2[i] for i in range(13))
    d2 = 0 if soma2 % 11 < 2 else 11 - soma2 % 11
    return int(cnpj[13]) == d2


def formatar_cpf(cpf: str) -> str:
    """Formata CPF: 123.456.789-00"""
    cpf = limpar_documento(cpf)
    if len(cpf) != 11:
        return cpf
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"


def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ: 12.345.678/0001-00"""
    cnpj = limpar_documento(cnpj)
    if len(cnpj) != 14:
        return cnpj
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


# ---------------------------------------------------------------------------
# Normalização de strings
# ---------------------------------------------------------------------------

def normalizar_nome(nome: str) -> str:
    """Remove acentos, upper, colapsa espaços."""
    if not nome or not isinstance(nome, str):
        return ""
    nome = unicodedata.normalize("NFD", nome)
    nome = "".join(c for c in nome if unicodedata.category(c) != "Mn")
    nome = re.sub(r"\s+", " ", nome.strip().upper())
    return nome


def similaridade_nomes(a: str, b: str) -> int:
    """Score de similaridade 0–100 via token_sort_ratio."""
    return fuzz.token_sort_ratio(normalizar_nome(a), normalizar_nome(b))


def mesmo_sobrenome(nome1: str, nome2: str) -> bool:
    """Verifica se o último sobrenome é igual."""
    p1 = normalizar_nome(nome1).split()
    p2 = normalizar_nome(nome2).split()
    if len(p1) < 2 or len(p2) < 2:
        return False
    return p1[-1] == p2[-1]


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Rate limiter simples por chave."""

    def __init__(self) -> None:
        self._timestamps: dict[str, list[float]] = defaultdict(list)

    def wait(self, key: str, max_per_minute: int) -> None:
        """Aguarda se necessário para respeitar o limite."""
        now = time.time()
        window = 60.0
        times = self._timestamps[key]
        # Limpa timestamps fora da janela
        self._timestamps[key] = [t for t in times if now - t < window]
        if len(self._timestamps[key]) >= max_per_minute:
            oldest = self._timestamps[key][0]
            sleep_time = window - (now - oldest) + 0.1
            if sleep_time > 0:
                time.sleep(sleep_time)
        self._timestamps[key].append(time.time())


# Instância global
rate_limiter = RateLimiter()


# ---------------------------------------------------------------------------
# Hash para cache
# ---------------------------------------------------------------------------

def hash_params(**params: Any) -> str:
    """Gera hash MD5 de parâmetros para cache key."""
    raw = "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Retry decorator simples
# ---------------------------------------------------------------------------

def retry_on_exception(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable:
    """Decorator de retry com backoff exponencial."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_retries:
                        time.sleep(current_delay)
                        current_delay *= backoff
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator
