"""
Script para tirar screenshots automáticas do dashboard ORUS.
Usa Playwright para navegar pelo dashboard e capturar todas as abas.
"""

import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

DASHBOARD_URL = "http://localhost:8502"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "screenshots"

TAB_NAMES = [
    "PAINEL",
    "DETALHES",
]


def wait_for_streamlit(page, timeout: int = 30_000):
    """Aguarda o Streamlit terminar de carregar."""
    try:
        # Espera o app Streamlit carregar (desaparece o "Please wait...")
        page.wait_for_selector('[data-testid="stApp"]', timeout=timeout)
        # Espera extra para renderização
        time.sleep(3)
    except PWTimeout:
        print("WARN: Timeout esperando Streamlit carregar")


def take_screenshots():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=1,
        )
        page = context.new_page()

        # --- 1. Navegar ao dashboard ---
        print(f"Abrindo {DASHBOARD_URL} ...")
        try:
            page.goto(DASHBOARD_URL, timeout=30_000)
        except Exception as e:
            print(f"ERRO: Não conseguiu acessar {DASHBOARD_URL}: {e}")
            browser.close()
            return results

        wait_for_streamlit(page)

        # --- 2. Screenshot da página principal (primeira aba visível) ---
        shot_path = OUTPUT_DIR / "01_pagina_principal.png"
        page.screenshot(path=str(shot_path), full_page=True)
        print(f"  [OK] {shot_path.name}")
        results["pagina_principal"] = str(shot_path)

        # --- 3. Checar se há erros visíveis na página ---
        error_elements = page.query_selector_all('[data-testid="stException"]')
        if error_elements:
            print(f"  [ERRO] Encontrados {len(error_elements)} erros na página principal!")
            for i, el in enumerate(error_elements):
                text = el.inner_text()[:300]
                print(f"    Erro {i+1}: {text}")
                results[f"erro_principal_{i}"] = text
        else:
            print("  [OK] Nenhum erro na página principal")

        # --- 4. Clicar em cada aba e tirar screenshot ---
        for idx, tab_name in enumerate(TAB_NAMES):
            print(f"\nClicando na aba '{tab_name}' ...")

            # Streamlit tabs: button[role="tab"]
            tab_buttons = page.query_selector_all('button[role="tab"]')

            if idx >= len(tab_buttons):
                print(f"  [WARN] Aba '{tab_name}' não encontrada (só {len(tab_buttons)} abas)")
                results[f"tab_{tab_name}"] = "NAO_ENCONTRADA"
                continue

            tab_buttons[idx].click()
            time.sleep(3)  # Espera renderização

            # Screenshot
            shot_num = str(idx + 2).zfill(2)
            shot_path = OUTPUT_DIR / f"{shot_num}_tab_{tab_name.lower().replace(' ', '_')}.png"
            page.screenshot(path=str(shot_path), full_page=True)
            print(f"  [OK] {shot_path.name}")
            results[f"tab_{tab_name}"] = str(shot_path)

            # Checar erros nesta aba
            error_elements = page.query_selector_all('[data-testid="stException"]')
            if error_elements:
                print(f"  [ERRO] Encontrados {len(error_elements)} erros na aba '{tab_name}'!")
                for i, el in enumerate(error_elements):
                    text = el.inner_text()[:300]
                    print(f"    Erro {i+1}: {text}")
                    results[f"erro_{tab_name}_{i}"] = text
            else:
                print(f"  [OK] Nenhum erro na aba '{tab_name}'")

        # --- 5. Resumo ---
        print("\n" + "=" * 60)
        print("RESUMO DAS VERIFICAÇÕES")
        print("=" * 60)

        erros = [k for k in results if k.startswith("erro_")]
        tabs_ok = [k for k in results if k.startswith("tab_") and results[k] != "NAO_ENCONTRADA"
                   and not k.replace("tab_", "").startswith("erro")]
        tabs_nf = [k for k in results if results.get(k) == "NAO_ENCONTRADA"]

        print(f"  Screenshots tiradas: {len([k for k in results if not k.startswith('erro')])}")
        print(f"  Abas OK: {len(tabs_ok)}")
        print(f"  Abas não encontradas: {len(tabs_nf)}")
        print(f"  Erros encontrados: {len(erros)}")

        if erros:
            print("\n  ERROS DETECTADOS:")
            for e in erros:
                print(f"    - {e}: {results[e][:200]}")
        else:
            print("\n  ✓ DASHBOARD 100% FUNCIONAL — SEM ERROS!")

        print(f"\n  Screenshots salvas em: {OUTPUT_DIR}")

        browser.close()

    return results


if __name__ == "__main__":
    take_screenshots()
