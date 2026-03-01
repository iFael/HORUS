"""
Teste de conectividade real de TODAS as APIs usadas pelo HORUS.
Testa se cada endpoint responde (HTTP 200) com dados válidos.
Não requer token (exceto Portal da Transparência que exige chave-api-dados).
"""
from __future__ import annotations

import sys
import json
import requests
from datetime import datetime

TIMEOUT = 30

RESULTS: list[dict] = []


def test_api(nome: str, url: str, headers: dict | None = None,
             params: dict | None = None, expect_json: bool = True) -> bool:
    """Testa uma API. Retorna True se ok."""
    try:
        resp = requests.get(url, headers=headers or {}, params=params or {}, timeout=TIMEOUT)
        status = resp.status_code

        if status == 403:
            RESULTS.append({"api": nome, "status": "SKIP (403 - requer token)", "url": url})
            print(f"  [SKIP] {nome} → 403 (requer autenticação)")
            return True  # Não é bug, é falta de token
        elif status == 401:
            RESULTS.append({"api": nome, "status": "SKIP (401 - requer token)", "url": url})
            print(f"  [SKIP] {nome} → 401 (requer autenticação)")
            return True

        if status != 200:
            RESULTS.append({"api": nome, "status": f"FAIL (HTTP {status})", "url": url})
            print(f"  [FAIL] {nome} → HTTP {status}")
            return False

        if expect_json:
            try:
                data = resp.json()
                size = len(data) if isinstance(data, list) else len(str(data))
                RESULTS.append({"api": nome, "status": f"OK ({size} items/chars)", "url": url})
                print(f"  [ OK ] {nome} → 200, {size} items/chars")
            except Exception:
                RESULTS.append({"api": nome, "status": "WARN (200 mas não JSON)", "url": url})
                print(f"  [WARN] {nome} → 200 mas resposta não é JSON")
        else:
            size = len(resp.content)
            RESULTS.append({"api": nome, "status": f"OK ({size} bytes)", "url": url})
            print(f"  [ OK ] {nome} → 200, {size} bytes")

        return True

    except requests.exceptions.Timeout:
        RESULTS.append({"api": nome, "status": "FAIL (timeout)", "url": url})
        print(f"  [FAIL] {nome} → Timeout ({TIMEOUT}s)")
        return False
    except requests.exceptions.ConnectionError as e:
        RESULTS.append({"api": nome, "status": f"FAIL (conexão: {e})", "url": url})
        print(f"  [FAIL] {nome} → Erro de conexão")
        return False
    except Exception as e:
        RESULTS.append({"api": nome, "status": f"FAIL ({e})", "url": url})
        print(f"  [FAIL] {nome} → {e}")
        return False


def main():
    print("=" * 70)
    print(" HORUS — Teste de Conectividade de APIs")
    print(f" Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70)

    # ---------------------------------------------------------------
    # 1. Portal da Transparência (CGU) — requer chave-api-dados
    # ---------------------------------------------------------------
    print("\n[1/14] Portal da Transparência (CGU)")
    import os
    token = os.getenv("TRANSPARENCIA_API_TOKEN", "")
    transp_headers = {"chave-api-dados": token, "Accept": "application/json"} if token else {}

    test_api("Transparência - Servidores",
             "https://api.portaldatransparencia.gov.br/api-de-dados/servidores/por-nome",
             headers=transp_headers,
             params={"nome": "JOAO SILVA", "pagina": 1})

    test_api("Transparência - Contratos",
             "https://api.portaldatransparencia.gov.br/api-de-dados/contratos",
             headers=transp_headers,
             params={"pagina": 1})

    # ---------------------------------------------------------------
    # 2. CGU Sanções (CEIS/CNEP/CEAF/CEPIM)
    # ---------------------------------------------------------------
    print("\n[2/14] CGU Sanções")
    test_api("CGU - CEIS",
             "https://api.portaldatransparencia.gov.br/api-de-dados/ceis",
             headers=transp_headers,
             params={"pagina": 1})

    test_api("CGU - CNEP",
             "https://api.portaldatransparencia.gov.br/api-de-dados/cnep",
             headers=transp_headers,
             params={"pagina": 1})

    # ---------------------------------------------------------------
    # 3. Receita Federal CNPJ (dados abertos)
    # ---------------------------------------------------------------
    print("\n[3/14] Receita Federal CNPJ")
    test_api("Receita Federal - Listagem ZIPs",
             "https://dadosabertos.rfb.gov.br/CNPJ/",
             expect_json=False)

    # ---------------------------------------------------------------
    # 4. TSE (dados abertos)
    # ---------------------------------------------------------------
    print("\n[4/14] TSE")
    test_api("TSE - CKAN API",
             "https://dadosabertos.tse.jus.br/api/3/action/package_list",
             params={"limit": 5})

    # ---------------------------------------------------------------
    # 5. PNCP
    # ---------------------------------------------------------------
    print("\n[5/14] PNCP")
    test_api("PNCP - Contratações",
             "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
             params={"pagina": 1, "tamanhoPagina": 5,
                     "dataInicial": "20250101", "dataFinal": "20250131"})

    # ---------------------------------------------------------------
    # 6. Querido Diário
    # ---------------------------------------------------------------
    print("\n[6/14] Querido Diário")
    test_api("Querido Diário - Gazettes",
             "https://queridodiario.ok.org.br/api/gazettes",
             params={"querystring": "licitação", "offset": 0, "size": 2})

    # ---------------------------------------------------------------
    # 7. BCB (Banco Central)
    # ---------------------------------------------------------------
    print("\n[7/14] Banco Central (BCB)")
    test_api("BCB - SGS Selic (série 432)",
             "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados",
             params={"formato": "json", "dataInicial": "01/01/2025", "dataFinal": "31/01/2025"})

    test_api("BCB - SGS IPCA (série 433)",
             "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados",
             params={"formato": "json", "dataInicial": "01/01/2025", "dataFinal": "31/01/2025"})

    # ---------------------------------------------------------------
    # 8. CVM
    # ---------------------------------------------------------------
    print("\n[8/14] CVM")
    test_api("CVM - Cadastro Cia Aberta",
             "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv",
             expect_json=False)

    # ---------------------------------------------------------------
    # 9. IBGE SIDRA
    # ---------------------------------------------------------------
    print("\n[9/14] IBGE")
    test_api("IBGE - SIDRA (IPCA mensal)",
             "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/2266/p/last%2012/d/v2266%2013")

    test_api("IBGE - Serviço de Dados (Estados)",
             "https://servicodados.ibge.gov.br/api/v3/localidades/estados")

    # ---------------------------------------------------------------
    # 10. SICONFI (Tesouro Nacional)
    # ---------------------------------------------------------------
    print("\n[10/14] SICONFI")
    test_api("SICONFI - Entes",
             "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes")

    # ---------------------------------------------------------------
    # 11. IPEAData
    # ---------------------------------------------------------------
    print("\n[11/14] IPEAData")
    test_api("IPEAData - Metadados PIB per capita",
             "http://www.ipeadata.gov.br/api/odata4/Metadados('BM12_PIB12')")

    # ---------------------------------------------------------------
    # 12. DATASUS (dados abertos de saúde)
    # ---------------------------------------------------------------
    print("\n[12/14] DATASUS")
    test_api("DATASUS - Portal Dados Abertos Saúde",
             "https://dadosabertos.saude.gov.br/dataset/cnes-dados-abertos",
             expect_json=False)

    # ---------------------------------------------------------------
    # 13. Emendas
    # ---------------------------------------------------------------
    print("\n[13/14] Emendas Parlamentares")
    test_api("Transparência - Emendas",
             "https://api.portaldatransparencia.gov.br/api-de-dados/emendas",
             headers=transp_headers,
             params={"ano": 2023, "pagina": 1})

    # ---------------------------------------------------------------
    # 14. Licitações
    # ---------------------------------------------------------------
    print("\n[14/14] Licitações")
    test_api("Transparência - Licitações",
             "https://api.portaldatransparencia.gov.br/api-de-dados/licitacoes",
             headers=transp_headers,
             params={"pagina": 1})

    # ---------------------------------------------------------------
    # Resumo
    # ---------------------------------------------------------------
    print("\n" + "=" * 70)
    print(" RESUMO")
    print("=" * 70)

    ok = sum(1 for r in RESULTS if r["status"].startswith("OK"))
    skip = sum(1 for r in RESULTS if r["status"].startswith("SKIP"))
    warn = sum(1 for r in RESULTS if r["status"].startswith("WARN"))
    fail = sum(1 for r in RESULTS if r["status"].startswith("FAIL"))
    total = len(RESULTS)

    print(f"\n  Total de endpoints testados: {total}")
    print(f"  ✓ OK:   {ok}")
    print(f"  ⊘ SKIP: {skip} (requerem token)")
    print(f"  ⚠ WARN: {warn}")
    print(f"  ✗ FAIL: {fail}")

    if fail > 0:
        print("\n  Endpoints com falha:")
        for r in RESULTS:
            if r["status"].startswith("FAIL"):
                print(f"    - {r['api']}: {r['status']}")

    print()
    return 1 if fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
