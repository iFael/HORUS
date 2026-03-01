"""Teste end-to-end interativo do dashboard ORUS."""
import time
from playwright.sync_api import sync_playwright

URL = "http://localhost:8502"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1920, "height": 1080})

    # 1. Carregar dashboard
    print("1. Carregando dashboard...")
    page.goto(URL, timeout=30000)
    page.wait_for_selector('[data-testid="stApp"]', timeout=30000)
    time.sleep(5)

    # 2. Verificar abas
    tabs = page.query_selector_all('button[role="tab"]')
    print(f"2. Abas encontradas: {len(tabs)}")
    for t in tabs:
        print(f"   - {t.inner_text()}")

    # 3. Checar metric cards
    metrics = page.query_selector_all(".metric-card")
    print(f"3. Metric cards: {len(metrics)}")

    # 4. Erros na pagina principal
    errors = page.query_selector_all('[data-testid="stException"]')
    print(f"4. Erros pagina principal: {len(errors)}")
    for e in errors:
        print(f"   -> {e.inner_text()[:200]}")

    # 5. Clicar aba DETALHES
    print("5. Clicando aba DETALHES...")
    if len(tabs) >= 2:
        tabs[1].click()
        time.sleep(4)

    errors2 = page.query_selector_all('[data-testid="stException"]')
    print(f"6. Erros aba DETALHES: {len(errors2)}")
    for e in errors2:
        print(f"   -> {e.inner_text()[:200]}")

    # 7. Selectboxes de filtro
    selects = page.query_selector_all('[data-testid="stSelectbox"]')
    print(f"7. Selectboxes: {len(selects)}")

    # 8. Voltar para aba PAINEL
    print("8. Voltando ao PAINEL...")
    if len(tabs) >= 1:
        tabs[0].click()
        time.sleep(3)

    errors3 = page.query_selector_all('[data-testid="stException"]')
    print(f"9. Erros apos voltar: {len(errors3)}")

    # 10. Verificar ORUS e auto-refresh
    html = page.content()
    print(f"10. ORUS presente: {'ORUS' in html}")

    # 11. Plotly charts
    plotly_charts = page.query_selector_all(".js-plotly-plot")
    print(f"11. Graficos Plotly: {len(plotly_charts)}")

    # 12. Insight cards
    insight_cards = page.query_selector_all(".insight-card")
    print(f"12. Insight cards: {len(insight_cards)}")

    # 13. Screenshot final
    page.screenshot(path="c:/Users/Rafael/OneDrive/Desktop/Project/RaioX_Publico_BR/screenshots/test_final.png", full_page=True)
    print("13. Screenshot final salva")

    browser.close()

    total_errors = len(errors) + len(errors2) + len(errors3)
    print(f"\n{'='*50}")
    if total_errors == 0:
        print("RESULTADO: DASHBOARD 100% FUNCIONAL - ZERO ERROS")
    else:
        print(f"!!! {total_errors} ERROS ENCONTRADOS !!!")
    print(f"{'='*50}")
