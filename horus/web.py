"""Dashboard HORUS — Interface de inteligência para rastreamento de políticos.

O dashboard é apenas a camada de visualização.
O HorusScheduler roda em background e coleta/analisa dados automaticamente.

LAYOUT: 5 abas (INSIGHTS, ANALYTICS, POLÍTICOS, SCANNER, BASE DE DADOS) — tudo atualiza em tempo real.
"""

from __future__ import annotations

import json
import math
import sys
import time
from pathlib import Path
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as components
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from streamlit_autorefresh import st_autorefresh  # type: ignore[import-untyped]

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from horus.config import Config
from horus.database import DatabaseManager
from horus.insights import InsightManager, Severidade, formatar_valor
from horus.scheduler import HorusScheduler
from horus.utils import get_logger

logger = get_logger(__name__)

# =====================================================================
# Page config
# =====================================================================
st.set_page_config(
    page_title="HORUS — Rastreamento Público",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =====================================================================
# AUTO-REFRESH — atualiza a cada 30 segundos sem interação manual
# =====================================================================
st_autorefresh(interval=30_000, limit=None, key="horus_autorefresh")

# =====================================================================
# Singletons
# =====================================================================

@st.cache_resource
def get_db():
    return DatabaseManager(Config())

@st.cache_resource
def get_insight_manager():
    return InsightManager(get_db())

@st.cache_resource
def get_scheduler():
    """Inicia o scheduler autônomo (singleton, roda em background)."""
    db = get_db()
    sched = HorusScheduler(db)
    sched.start(
        run_initial_scan=True,
        full_interval_hours=6,
        quick_interval_hours=1,
        refresh_interval_minutes=15,
    )
    return sched

# =====================================================================
# CSS — TEMA DARK PREMIUM
# =====================================================================

_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@300;400;500;600;700;800;900&display=swap');

:root {
    --bg-primary: #080b16;
    --bg-secondary: #0d1117;
    --bg-card: rgba(15, 20, 35, 0.8);
    --bg-card-hover: rgba(20, 28, 50, 0.9);
    --border: rgba(60, 70, 120, 0.25);
    --border-glow: rgba(100, 120, 200, 0.3);
    --text-primary: #ffffff;
    --text-secondary: #e0e4ef;
    --text-muted: #b0b8cc;
    --accent-red: #ff2d55;
    --accent-orange: #ff6b35;
    --accent-amber: #ffb800;
    --accent-green: #22c55e;
    --accent-blue: #3b82f6;
    --gradient-red: linear-gradient(135deg, #ff2d55 0%, #ff6b35 100%);
    --gradient-orange: linear-gradient(135deg, #ff6b35 0%, #ffb800 100%);
    --gradient-green: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
}

.stApp {
    background: linear-gradient(135deg, #080b16 0%, #0f1628 25%, #141e3a 50%, #0f1628 75%, #080b16 100%) !important;
    background-attachment: fixed !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', -apple-system, sans-serif !important;
}
.stApp > header { display: none !important; height: 0 !important; min-height: 0 !important; }
.block-container { padding-top: 0 !important; margin-top: 0 !important; }
.stApp [data-testid="stHeader"] { display: none !important; }
#MainMenu, footer, .stDeployButton { display: none !important; }
div[data-testid="stToolbar"] { display: none !important; }
div[data-testid="stDecoration"] { display: none !important; }
div[data-testid="stStatusWidget"] { display: none !important; }
iframe[title="streamlit_autorefresh.st_autorefresh"] { display: none !important; height: 0 !important; }
/* Eliminar qualquer barra colorida residual do Streamlit */
.stApp::before, .stApp::after { display: none !important; }
header[data-testid="stHeader"] { display: none !important; height: 0 !important; }
.stAppDeployButton { display: none !important; }
section[data-testid="stAppViewBlockContainer"] { padding-top: 0 !important; }

/* Cursor padrão em toda a página — remove o I-beam (cursor de texto) */
*, *::before, *::after { cursor: default !important; }
a, button, [role="button"], .stSelectbox, select, input, summary, label[data-testid] { cursor: pointer !important; }
input[type="text"], input[type="number"], textarea { cursor: text !important; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #080b16; }
::-webkit-scrollbar-thumb { background: rgba(60,70,120,0.4); border-radius: 3px; }

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1117 0%, #141e3a 100%) !important;
    border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] * { color: var(--text-primary) !important; }

.topbar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 0; border-bottom: 1px solid var(--border); margin-bottom: 0;
}
.topbar-brand { display: flex; align-items: center; gap: 12px; }
.topbar-brand .logo {
    width: 36px; height: 36px; background: var(--gradient-orange); border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px; font-weight: 900; color: #000;
}
.topbar-brand .name {
    font-family: 'JetBrains Mono', monospace; font-size: 20px; font-weight: 700;
    letter-spacing: 3px; background: var(--gradient-orange);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.topbar-stats { display: flex; gap: 32px; align-items: center; }
.topbar-stat { text-align: center; }
.topbar-stat .value {
    font-family: 'JetBrains Mono', monospace; font-size: 24px; font-weight: 800;
    color: var(--accent-amber); line-height: 1;
}
.topbar-stat .label {
    font-size: 9px; font-weight: 600; letter-spacing: 2px;
    text-transform: uppercase; color: var(--text-muted); margin-top: 2px;
}
.topbar-live {
    display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: var(--accent-green); font-weight: 600; letter-spacing: 1px;
}
.topbar-live::before {
    content: ''; width: 8px; height: 8px; background: var(--accent-green);
    border-radius: 50%; animation: pulse 2s infinite;
}
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

.card {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px;
    padding: 20px; margin-bottom: 12px; transition: all 0.25s ease;
}
.card:hover { background: var(--bg-card-hover); border-color: var(--border-glow); box-shadow: 0 4px 20px rgba(0,0,0,0.3), 0 0 15px rgba(60,70,120,0.1); }

.metric-card {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px;
    padding: 20px 24px; position: relative; overflow: hidden;
    display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center;
    transition: all 0.25s ease;
}
.metric-card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.3), 0 0 15px rgba(60,70,120,0.1); border-color: var(--border-glow); }
.metric-card::before {
    content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 3px;
}
.metric-card.red::before { background: var(--gradient-red); }
.metric-card.orange::before { background: var(--gradient-orange); }
.metric-card.green::before { background: var(--gradient-green); }
.metric-card.blue::before { background: linear-gradient(90deg, #3b82f6, #8b5cf6); }
.metric-card .metric-value {
    font-family: 'JetBrains Mono', monospace; font-size: 32px; font-weight: 800;
    line-height: 1; margin-bottom: 4px;
}
.metric-card.red .metric-value { color: var(--accent-red); }
.metric-card.orange .metric-value { color: var(--accent-orange); }
.metric-card.green .metric-value { color: var(--accent-green); }
.metric-card.blue .metric-value { color: var(--accent-blue); }
.metric-card .metric-label {
    font-size: 11px; font-weight: 600; letter-spacing: 2px;
    text-transform: uppercase; color: var(--text-secondary);
}

.insight-card {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px;
    padding: 20px 24px; margin-bottom: 10px; position: relative;
    border-left: 3px solid transparent; transition: all 0.25s ease;
}
.insight-card:hover { background: var(--bg-card-hover); transform: translateX(2px); box-shadow: 0 4px 20px rgba(0,0,0,0.3), 0 0 15px rgba(60,70,120,0.1); }
.insight-card.CRITICO { border-left-color: var(--accent-red); }
.insight-card.ALTO { border-left-color: var(--accent-orange); }
.insight-card.MEDIO { border-left-color: var(--accent-amber); }
.insight-card.BAIXO { border-left-color: var(--accent-green); }
.insight-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
.insight-badge {
    display: inline-block; padding: 3px 10px; border-radius: 4px;
    font-size: 10px; font-weight: 800; letter-spacing: 1.5px;
}
.insight-badge.CRITICO { background: rgba(255,45,45,0.15); color: var(--accent-red); border: 1px solid rgba(255,45,45,0.3); }
.insight-badge.ALTO { background: rgba(255,107,53,0.15); color: var(--accent-orange); border: 1px solid rgba(255,107,53,0.3); }
.insight-badge.MEDIO { background: rgba(255,184,0,0.15); color: var(--accent-amber); border: 1px solid rgba(255,184,0,0.3); }
.insight-badge.BAIXO { background: rgba(34,197,94,0.15); color: var(--accent-green); border: 1px solid rgba(34,197,94,0.3); }
.insight-score { font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 700; }
.insight-score.CRITICO { color: var(--accent-red); }
.insight-score.ALTO { color: var(--accent-orange); }
.insight-score.MEDIO { color: var(--accent-amber); }
.score-bar {
    width: 60px; height: 4px; background: #222; border-radius: 2px;
    overflow: hidden; display: inline-block; vertical-align: middle; margin-right: 8px;
}
.score-bar-fill { height: 100%; border-radius: 2px; }
.score-bar-fill.CRITICO { background: var(--accent-red); }
.score-bar-fill.ALTO { background: var(--accent-orange); }
.score-bar-fill.MEDIO { background: var(--accent-amber); }
.score-bar-fill.BAIXO { background: var(--accent-green); }
.insight-title { font-size: 16px; font-weight: 700; color: var(--text-primary); margin-bottom: 4px; }
.insight-value {
    font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 600;
    color: var(--accent-amber); margin-bottom: 8px;
}
.insight-desc { font-size: 13px; line-height: 1.6; color: var(--text-secondary); margin-bottom: 12px; }
.insight-pattern {
    background: rgba(15,20,35,0.6); border: 1px solid var(--border); border-radius: 6px;
    padding: 10px 14px; margin-bottom: 10px;
}
.insight-pattern-label { font-size: 9px; font-weight: 700; letter-spacing: 2px; color: var(--text-secondary); margin-bottom: 4px; }
.insight-pattern-text { font-family: 'JetBrains Mono', monospace; font-size: 12px; font-weight: 600; color: var(--accent-orange); }
.insight-tags { display: flex; gap: 6px; flex-wrap: wrap; }
.insight-tag { font-size: 10px; padding: 2px 8px; border-radius: 3px; background: rgba(15,20,35,0.6); border: 1px solid var(--border); color: var(--text-secondary); }

.exposure-banner {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px;
    padding: 24px 28px; margin-bottom: 16px; position: relative; overflow: hidden;
}
.exposure-banner::before {
    content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 100%;
    background: linear-gradient(135deg, rgba(255,45,45,0.05) 0%, rgba(255,107,53,0.03) 100%);
    pointer-events: none;
}
.exposure-banner { transition: all 0.25s ease; }
.exposure-banner:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.3), 0 0 15px rgba(60,70,120,0.1); border-color: var(--border-glow); }
.exposure-label { font-size: 10px; font-weight: 700; letter-spacing: 3px; text-transform: uppercase; color: var(--accent-red); margin-bottom: 4px; }
.exposure-value { font-family: 'JetBrains Mono', monospace; font-size: 42px; font-weight: 900; color: var(--text-primary); line-height: 1; margin-bottom: 6px; }
.exposure-meta { font-size: 13px; color: var(--text-secondary); }

.stTabs [data-baseweb="tab-list"] { background: rgba(8,11,22,0.95) !important; border-bottom: 1px solid var(--border) !important; gap: 0 !important; position: sticky !important; top: 0 !important; z-index: 100 !important; backdrop-filter: blur(12px) !important; padding-top: 4px !important; }
.stTabs [data-baseweb="tab"] { background: transparent !important; color: var(--text-secondary) !important; border: none !important; font-weight: 600 !important; font-size: 13px !important; letter-spacing: 1px !important; padding: 10px 20px !important; }
.stTabs [aria-selected="true"] { color: var(--accent-orange) !important; border-bottom: 2px solid var(--accent-orange) !important; }

.filter-pills { display: flex; gap: 8px; margin-bottom: 16px; }
.pill { padding: 5px 14px; border-radius: 20px; font-size: 12px; font-weight: 600; border: 1px solid var(--border); color: var(--text-secondary); cursor: pointer; transition: all 0.15s; }
.pill.active { background: rgba(255,107,53,0.15); border-color: var(--accent-orange); color: var(--accent-orange); }

.ranking-table { width: 100%; border-collapse: collapse; }
.ranking-table th { font-size: 10px; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; color: var(--text-secondary); text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); background: var(--bg-secondary); position: sticky; top: 0; z-index: 2; }
.ranking-table td { font-size: 13px; padding: 12px; border-bottom: 1px solid rgba(60,70,120,0.15); color: var(--text-primary); }
.ranking-table tr:hover td { background: var(--bg-card-hover); box-shadow: inset 0 0 15px rgba(60,70,120,0.08); }
.table-scroll-container { max-height: 70vh; overflow-y: auto; border: 1px solid var(--border); border-radius: 12px; background: var(--bg-card); }

.section-header { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; color: var(--text-secondary); margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }

.stButton > button { background: linear-gradient(135deg, #ff6b35 0%, #ff2d2d 100%) !important; color: #000 !important; font-weight: 700 !important; border: none !important; border-radius: 8px !important; padding: 8px 24px !important; font-size: 13px !important; letter-spacing: 1px !important; transition: opacity 0.2s !important; }
.stButton > button:hover { opacity: 0.85 !important; }
.stSelectbox > div > div, .stMultiSelect > div > div { background: var(--bg-card) !important; border-color: var(--border) !important; color: var(--text-primary) !important; }
.stSelectbox input { caret-color: transparent !important; }
.stTextInput > div > div { background: var(--bg-card) !important; border-color: var(--border) !important; color: var(--text-primary) !important; border-radius: 10px !important; }
.stTextInput input { color: var(--text-primary) !important; font-size: 14px !important; }
.stTextInput input::placeholder { color: var(--text-muted) !important; opacity: 0.7 !important; }
div[data-testid="stMetric"] { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 16px !important; }
div[data-testid="stMetric"] label { color: var(--text-muted) !important; }
div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace !important; color: var(--text-primary) !important; }
.stSpinner > div { border-top-color: var(--accent-orange) !important; }
.js-plotly-plot .plotly .modebar { display: none !important; }
.js-plotly-plot .plotly .nsewdrag,
.js-plotly-plot .plotly .drag,
.js-plotly-plot .plotly .draglayer { cursor: default !important; }
.js-plotly-plot .plotly .plot-container { cursor: default !important; }
/* Relógio + próxima atualização flutuante sobre a linha das abas */
.clock-row { margin-bottom: -40px; position: sticky; top: 0; z-index: 101; pointer-events: none; background: rgba(8,11,22,0.95); backdrop-filter: blur(12px); }
/* Impedir digitação nos selectbox — somente clicar para selecionar */
[data-baseweb="select"] input { caret-color: transparent !important; ime-mode: disabled !important; }
</style>
<script>
(function() {
    function lockSelects() {
        document.querySelectorAll('[data-baseweb="select"] input').forEach(function(el) {
            el.setAttribute('readonly', 'true');
            el.setAttribute('inputmode', 'none');
            el.setAttribute('autocomplete', 'off');
        });
    }
    lockSelects();
    new MutationObserver(lockSelects).observe(document.body, {childList: true, subtree: true});

    /* Toggle dropdown: clicar no campo abre; clicar de novo fecha */
    document.addEventListener('click', function(e) {
        var selectInput = e.target.closest('[data-baseweb="select"] input');
        if (!selectInput) return;
        var selectContainer = selectInput.closest('[data-baseweb="select"]');
        if (!selectContainer) return;
        var listbox = document.querySelector('[data-baseweb="popover"], [role="listbox"]');
        if (listbox && listbox.offsetParent !== null) {
            /* Dropdown já está aberto — fechar via blur */
            selectInput.blur();
            e.preventDefault();
            e.stopPropagation();
        }
    }, true);
})();
</script>
"""

st.markdown(_CSS, unsafe_allow_html=True)

# =====================================================================
# PLOTLY THEME
# =====================================================================

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color="#e0e4ef"),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor="rgba(60,70,120,0.15)", zerolinecolor="rgba(60,70,120,0.15)", fixedrange=True),
    yaxis=dict(gridcolor="rgba(60,70,120,0.15)", zerolinecolor="rgba(60,70,120,0.15)", fixedrange=True),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#8892a8")),
    dragmode=False,
)

COLORS = {
    "CRITICO": "#ff2d2d",
    "ALTO": "#ff6b35",
    "MEDIO": "#ffb800",
    "BAIXO": "#22c55e",
}

# =====================================================================
# HELPER FUNCTIONS
# =====================================================================

def render_topbar(stats: dict):
    n_pol = stats.get("politicos", 0)
    n_ins = stats.get("insights", 0)
    # Contar fontes ativas do registry
    try:
        from horus.etl.registry import get_registry, ETLStatus
        n_fontes = sum(1 for e in get_registry() if e.status != ETLStatus.INATIVO)
    except Exception:
        n_fontes = stats.get("fontes", 0) or 16
    n_alertas = stats.get("alertas", 0)

    # Scheduler status para indicador LIVE
    sched = get_scheduler()
    sched_status = sched.status
    is_active = sched_status.get("running", False)
    current_task = sched_status.get("current_task")
    uptime = sched.get_uptime()

    live_color = "#22c55e" if is_active else "#ff2d2d"
    live_text = current_task if current_task else ("LIVE" if is_active else "OFF")
    live_pulse = 'animation: pulse 2s infinite;' if is_active else ''

    st.markdown(f"""
    <div class="topbar">
        <div class="topbar-brand">
            <div class="logo">&#9889;</div>
            <div class="name">HORUS</div>
        </div>
        <div class="topbar-stats">
            <div class="topbar-stat">
                <div class="value">{n_pol}</div>
                <div class="label">ENTIDADES</div>
            </div>
            <div class="topbar-stat">
                <div class="value">{n_ins}</div>
                <div class="label">CONEXÕES</div>
            </div>
            <div class="topbar-stat">
                <div class="value">{n_fontes}</div>
                <div class="label">FONTES</div>
            </div>
            <div class="topbar-stat">
                <div class="value">{n_alertas}</div>
                <div class="label">ALERTAS</div>
            </div>
        </div>
        <div class="topbar-live" style="color:{live_color};{live_pulse}">
            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{live_color};margin-right:6px;{live_pulse}"></span>
            {live_text}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_exposure_banner(exposicao: float, total_insights: int, sev_counts: dict):
    st.markdown(f"""
    <div class="exposure-banner">
        <div class="exposure-label">EXPOSIÇÃO TOTAL</div>
        <div class="exposure-value">{formatar_valor(exposicao)}</div>
        <div class="exposure-meta">
            {total_insights} irregularidades &nbsp;&middot;&nbsp; {n_fontes} fontes
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_severity_summary(sev_counts: dict):
    """Exibe um resumo compacto das severidades (somente visual, sem interação)."""
    total = sum(sev_counts.values())
    parts = [f'<span style="color:#ffffff;font-weight:700;">{total} total</span>']
    sev_labels = {"CRITICO": "Crítico", "ALTO": "Alto", "MEDIO": "Médio", "BAIXO": "Baixo"}
    mapping = {"CRITICO": "#ff2d2d", "ALTO": "#ff6b35", "MEDIO": "#ffb800", "BAIXO": "#22c55e"}
    for sev, color in mapping.items():
        cnt = sev_counts.get(sev, 0)
        if cnt > 0:
            parts.append(f'<span style="color:{color};font-weight:600;">{cnt} {sev_labels[sev]}</span>')
    st.markdown(
        f'<div style="font-size:13px;color:#b0b8cc;margin-bottom:12px;display:flex;gap:16px;align-items:center;">'
        + ' &middot; '.join(parts) + '</div>',
        unsafe_allow_html=True,
    )


def render_insight_card(insight: dict):
    sev = insight.get("severidade", "MEDIO")
    score = insight.get("score", 0)
    titulo = insight.get("titulo", "")
    desc = insight.get("descricao", "")
    valor = insight.get("valor_exposicao", 0)
    pattern = insight.get("pattern", "")
    fontes = insight.get("fontes", "")

    valor_html = ""
    if valor:
        valor_html = '<div class="insight-value">' + formatar_valor(valor) + '</div>'

    pattern_html = ""
    if pattern:
        pattern_html = (
            '<div class="insight-pattern">'
            '<div class="insight-pattern-label">INDICADOR</div>'
            f'<div class="insight-pattern-text">{pattern}</div>'
            '</div>'
        )

    tags_html = ""
    if fontes:
        tags = fontes.split(",") if isinstance(fontes, str) else fontes
        tags_html = '<div class="insight-tags">' + "".join(
            f'<span class="insight-tag">{t.strip()}</span>' for t in tags
        ) + '</div>'

    pct = int(score)
    st.markdown(
        f'<div class="insight-card {sev}">'
        f'<div class="insight-header">'
        f'<span class="insight-badge {sev}">{sev}</span>'
        f'<span class="insight-score {sev}">'
        f'<span class="score-bar"><span class="score-bar-fill {sev}" style="width:{pct}%"></span></span>'
        f' {pct}%'
        f'</span></div>'
        f'<div class="insight-title">{titulo}</div>'
        f'{valor_html}'
        f'<div class="insight-desc">{desc}</div>'
        f'{pattern_html}'
        f'{tags_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def make_severity_donut(sev_counts: dict) -> go.Figure:
    labels = []
    values = []
    colors = []
    sev_labels = {"CRITICO": "Crítico", "ALTO": "Alto", "MEDIO": "Médio", "BAIXO": "Baixo"}
    for sev in ["CRITICO", "ALTO", "MEDIO", "BAIXO"]:
        cnt = sev_counts.get(sev, 0)
        if cnt > 0:
            labels.append(sev_labels[sev])
            values.append(cnt)
            colors.append(COLORS[sev])

    if not values:
        labels, values, colors = ["Sem dados"], [1], ["#333"]

    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.7,
        marker=dict(colors=colors, line=dict(color="#0a0a0a", width=2)),
        textinfo="none",
        hovertemplate="%{label}: %{value}<extra></extra>",
    ))
    total = sum(values)
    fig.update_layout(**PLOTLY_LAYOUT)
    fig.update_layout(
        height=260,
        showlegend=True,
        legend=dict(orientation="v", x=1.05, y=0.5, font=dict(size=11)),
        margin=dict(l=10, r=80, t=20, b=20),
        annotations=[dict(
            text=f"<b>{total}</b><br><span style='font-size:10px;color:#b0b8cc'>TOTAL</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=28, color="#e8e8e8", family="JetBrains Mono"),
        )],
    )
    return fig


def _brl_ticks(values: list) -> dict:
    """Gera tickvals/ticktext em formato R$ para eixos monetários."""
    max_val = max(values) if values else 0
    if max_val <= 0:
        return dict(tickvals=[0], ticktext=["R$0"])

    # Escala e step
    exp = int(math.log10(max_val))
    base = 10 ** exp
    for nice in [1, 2, 2.5, 5, 10]:
        step = nice * (base / 10)
        if step * 5 >= max_val * 0.8:
            break

    ticks, texts = [], []
    v = 0.0
    while v <= max_val * 1.05:
        ticks.append(v)
        if v == 0:
            texts.append("R$0")
        elif v >= 1e9:
            b = v / 1e9
            texts.append(f"R${b:g}Bi")
        elif v >= 1e6:
            texts.append(f"R${v / 1e6:g}M")
        elif v >= 1e3:
            texts.append(f"R${v / 1e3:g}K")
        else:
            texts.append(f"R${v:,.0f}")
        v += step
    return dict(tickvals=ticks, ticktext=texts)


def make_exposure_by_type(insights: list) -> go.Figure:
    by_type: dict[str, float] = {}
    for ins in insights:
        tipo = ins.get("tipo", "outro")
        val = ins.get("valor_exposicao", 0)
        label = tipo.replace("_", " ").title()[:25]
        by_type[label] = by_type.get(label, 0) + val

    sorted_types = sorted(by_type.items(), key=lambda x: x[1], reverse=True)[:10]
    if not sorted_types:
        sorted_types = [("Sem dados", 0)]

    labels = [x[0] for x in sorted_types]
    vals = [x[1] for x in sorted_types]

    fig = go.Figure(go.Bar(
        x=vals, y=labels, orientation="h",
        marker=dict(
            color=vals,
            colorscale=[[0, "#ff6b35"], [0.5, "#ff2d2d"], [1, "#cc0000"]],
            line=dict(width=0),
            cornerradius=4,
        ),
        hovertemplate="%{y}: R$%{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(**PLOTLY_LAYOUT)
    tick_cfg = _brl_ticks(vals)
    fig.update_layout(
        height=300,
        yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor="#1a1a1a", **tick_cfg),
        margin=dict(l=160, r=20, t=10, b=40),
    )
    return fig


def make_top_politicos_chart(top_pols: list) -> go.Figure:
    # Filtrar entradas sem nome
    top_pols = [p for p in top_pols if p.get("politico_nome", "").strip()]
    if not top_pols:
        fig = go.Figure()
        fig.update_layout(**PLOTLY_LAYOUT, height=300)
        fig.add_annotation(
            text="Nenhum insight vinculado a político ainda",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=13, color="#6b7280"),
        )
        fig.update_xaxes(visible=False)
        fig.update_yaxes(visible=False)
        return fig

    nomes = [p["politico_nome"][:25] for p in top_pols[:10]]
    valores = [p.get("exposicao_total") or 0 for p in top_pols[:10]]

    fig = go.Figure(go.Bar(
        x=nomes, y=valores,
        marker=dict(
            color=valores,
            colorscale=[[0, "#ff6b35"], [0.5, "#ff2d2d"], [1, "#8b0000"]],
            line=dict(width=0),
            cornerradius=6,
        ),
        hovertemplate="%{x}: R$%{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(**PLOTLY_LAYOUT)
    tick_cfg = _brl_ticks(valores)
    fig.update_layout(
        height=300,
        xaxis=dict(tickangle=-45, gridcolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor="#1a1a1a", **tick_cfg),
        margin=dict(l=60, r=20, t=10, b=100),
    )
    return fig


def page_overview():
    """Página principal — Dashboard com 4 abas."""
    db = get_db()
    mgr = get_insight_manager()

    try:
        stats = db.get_dashboard_stats()
    except Exception:
        stats = {"politicos": 0, "insights": 0, "alertas": 0, "exposicao_total": 0, "fontes": 0}

    # Calcular próxima atualização para exibir no cabeçalho
    sched = get_scheduler()
    _sched_status = sched.status
    _next_times = []
    for _k in ("next_full_scan", "next_quick_scan", "next_refresh"):
        _iso = _sched_status.get(_k)
        if _iso:
            try:
                _dt = datetime.fromisoformat(_iso)
                _now = datetime.now(_dt.tzinfo) if _dt.tzinfo else datetime.now()
                _delta = _dt - _now
                _mins = max(0, int(_delta.total_seconds() / 60))
                _next_times.append(_mins)
            except Exception:
                pass
    _next_min = min(_next_times) if _next_times else None
    if _next_min is not None:
        if _next_min > 60:
            _next_label = f"{_next_min // 60}h {_next_min % 60}min"
        elif _next_min > 0:
            _next_label = f"{_next_min}min"
        else:
            _next_label = "agora"
    else:
        _next_label = "—"

    # Relógio + próxima atualização — posicionado sobre a linha das abas (direita)
    st.markdown('<div class="clock-row">', unsafe_allow_html=True)
    components.html(
        f"""
        <div style="display:flex;justify-content:flex-end;align-items:center;gap:18px;padding:4px 10px 0 0;font-family:'JetBrains Mono',monospace;">
            <div style="display:flex;flex-direction:column;align-items:center;">
                <span style="color:rgba(255,255,255,0.45);font-size:9px;letter-spacing:0.5px;">Próxima atualização em</span>
                <span style="color:#ff6b35;font-weight:600;font-size:12px;margin-top:1px;">{_next_label}</span>
            </div>
            <span style="color:rgba(255,255,255,0.15);font-size:18px;">|</span>
            <span id="horus-clock" style="font-weight:700;color:#ff6b35;font-size:14px;letter-spacing:3px;"></span>
        </div>
        <script>
        (function() {{
            function updateClock() {{
                var now = new Date();
                var h = String(now.getHours()).padStart(2, '0');
                var m = String(now.getMinutes()).padStart(2, '0');
                var s = String(now.getSeconds()).padStart(2, '0');
                var el = document.getElementById('horus-clock');
                if (el) el.textContent = h + ':' + m + ':' + s;
            }}
            updateClock();
            setInterval(updateClock, 1000);
        }})();
        </script>
        """,
        height=35,
    )
    st.markdown('</div>', unsafe_allow_html=True)

    tab_insights, tab_analytics, tab_politicos, tab_scanner, tab_fontes, tab_database = st.tabs(
        ["⚡ INSIGHTS", "📊 ANALYTICS", "👤 POLÍTICOS", "🛡 SCANNER", "📡 FONTES", "🗄 BASE DE DADOS"]
    )

    with tab_insights:
        _render_tab_insights(mgr, stats)

    with tab_analytics:
        _render_tab_analytics(mgr, stats)

    with tab_politicos:
        _render_tab_politicos(db)

    with tab_scanner:
        _render_tab_scanner(db)

    with tab_fontes:
        _render_tab_fontes()

    with tab_database:
        _render_tab_database(db)


def _render_tab_insights(mgr: InsightManager, stats: dict):
    """Aba INSIGHTS: métricas + cards de insights."""
    insights = mgr.get_todos(limite=200)
    sev_counts = mgr.get_contagem_severidade()
    exposicao = mgr.get_exposicao_total()

    # --- Métricas no topo ---
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            f'<div class="metric-card red"><div class="metric-value">{sev_counts.get("CRITICO", 0)}</div>'
            '<div class="metric-label">CRÍTICOS</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="metric-card orange"><div class="metric-value">{sev_counts.get("ALTO", 0)}</div>'
            '<div class="metric-label">ALTO RISCO</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f'<div class="metric-card green"><div class="metric-value">{stats.get("politicos", 0)}</div>'
            '<div class="metric-label">POLÍTICOS</div></div>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            f'<div class="metric-card blue"><div class="metric-value">{formatar_valor(exposicao)}</div>'
            '<div class="metric-label">EXPOSIÇÃO</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("")
    render_exposure_banner(exposicao, len(insights), sev_counts)
    render_severity_summary(sev_counts)

    if not insights:
        st.markdown(
            '<div class="card" style="text-align:center; padding:40px;">'
            '<div style="font-size:40px; margin-bottom:10px;">&#128269;</div>'
            '<div style="font-size:16px; font-weight:600; color:#e0e4ef;">'
            'Nenhum insight detectado ainda</div>'
            '<div style="font-size:13px; color:#b0b8cc; margin-top:6px;">'
            'O sistema está analisando dados automaticamente...</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        # --- Mapeamento de tipos para nomes amigáveis ---
        _TIPO_LABELS = {
            "circuito_doacao_contrato": "Circuito Doação → Contrato",
            "concentracao_fornecedor": "Concentração de Fornecedor",
            "despesa_atipica": "Despesa Atípica",
            "despesa_concentrada": "Despesa Concentrada",
            "fornecedor_doador": "Doador = Fornecedor",
            "fornecedor_sancionado": "Fornecedor Sancionado",
            "emenda_concentrada": "Emenda Concentrada",
            "valor_fracionado": "Fracionamento de Valor",
            "execucao_orcamentaria_anomala": "Execução Orçamentária Anômala",
        }
        tipos_presentes = sorted({i.get("tipo", "") for i in insights if i.get("tipo")})
        opcoes_indicador = ["Todos"] + [_TIPO_LABELS.get(t, t) for t in tipos_presentes]
        _label_to_tipo = {_TIPO_LABELS.get(t, t): t for t in tipos_presentes}

        # Severidades presentes
        sevs_presentes = sorted(
            {i.get("severidade", "") for i in insights if i.get("severidade")},
            key=lambda s: ["CRITICO", "ALTO", "MEDIO", "BAIXO"].index(s) if s in ["CRITICO", "ALTO", "MEDIO", "BAIXO"] else 99,
        )
        _SEV_LABELS = {"CRITICO": "Crítico", "ALTO": "Alto", "MEDIO": "Médio", "BAIXO": "Baixo"}
        opcoes_sev = ["Todos"] + [_SEV_LABELS.get(s, s) for s in sevs_presentes]
        _label_to_sev = {_SEV_LABELS.get(s, s): s for s in sevs_presentes}

        col_f1, col_f2, _ = st.columns([1, 1, 2])
        with col_f1:
            indicador_filter = st.selectbox(
                "Filtrar por indicador",
                opcoes_indicador,
                label_visibility="collapsed",
            )
        with col_f2:
            sev_filter = st.selectbox(
                "Filtrar por severidade",
                opcoes_sev,
                label_visibility="collapsed",
            )

        filtered = insights
        if indicador_filter != "Todos":
            tipo_sel = _label_to_tipo.get(indicador_filter, indicador_filter)
            filtered = [i for i in filtered if i.get("tipo") == tipo_sel]
        if sev_filter != "Todos":
            sev_sel = _label_to_sev.get(sev_filter, sev_filter)
            filtered = [i for i in filtered if i.get("severidade") == sev_sel]

        if not filtered:
            st.markdown(
                '<div class="card" style="text-align:center; padding:30px;">'
                '<div style="font-size:14px; color:#b0b8cc;">'
                'Nenhum insight encontrado com esses filtros.</div></div>',
                unsafe_allow_html=True,
            )
        for ins in filtered[:30]:
            render_insight_card(ins)


def _render_tab_analytics(mgr: InsightManager, stats: dict):
    """Aba ANALYTICS: gráficos e ranking."""
    insights = mgr.get_todos(limite=200)
    sev_counts = mgr.get_contagem_severidade()
    top_pols = mgr.get_top_politicos(10)

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<div class="section-header">Distribuição por Severidade</div>', unsafe_allow_html=True)
        st.plotly_chart(make_severity_donut(sev_counts), use_container_width=True, config={"displayModeBar": False, "scrollZoom": False, "editable": False, "staticPlot": False})

        st.markdown('<div class="section-header">Top Políticos por Exposição</div>', unsafe_allow_html=True)
        st.plotly_chart(make_top_politicos_chart(top_pols), use_container_width=True, config={"displayModeBar": False, "scrollZoom": False, "editable": False, "staticPlot": False})

    with col_right:
        st.markdown('<div class="section-header">Exposição por Tipo</div>', unsafe_allow_html=True)
        st.plotly_chart(make_exposure_by_type(insights), use_container_width=True, config={"displayModeBar": False, "scrollZoom": False, "editable": False, "staticPlot": False})

        st.markdown('<div class="section-header">Ranking de Exposição</div>', unsafe_allow_html=True)
        _render_ranking_table(top_pols)


def _render_tab_politicos(db: DatabaseManager):
    """Aba POLÍTICOS: tabela de políticos rastreados com filtros."""
    st.markdown('<div class="section-header">Políticos Rastreados</div>', unsafe_allow_html=True)

    all_pols = db.buscar_politicos(limite=1000)

    # Campo de busca por nome
    nome_busca = st.text_input(
        "\U0001F50D Buscar político por nome",
        placeholder="Ex: Nikolas Ferreira, Lula, Bolsonaro...",
        label_visibility="collapsed",
    )

    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        cargo_filter = st.selectbox("Cargo", ["Todos", "Deputado Federal", "Senador"])
    with fc2:
        ufs = sorted(set(p["uf"] for p in all_pols if p.get("uf")))
        uf_filter = st.selectbox("UF", ["Todos"] + ufs)
    with fc3:
        partidos = sorted(set(p["partido"] for p in all_pols if p.get("partido")))
        partido_filter = st.selectbox("Partido", ["Todos"] + partidos)
    with fc4:
        insight_filter = st.selectbox("Insights", ["Todos", "Com Insights", "Sem Insights"])

    kwargs: dict = {}
    if nome_busca.strip():
        kwargs["nome"] = nome_busca.strip()
    if cargo_filter != "Todos":
        kwargs["cargo"] = cargo_filter
    if uf_filter != "Todos":
        kwargs["uf"] = uf_filter
    if partido_filter != "Todos":
        kwargs["partido"] = partido_filter

    politicos = db.buscar_politicos(**kwargs, limite=5000)

    # Filtro de insights — aplicado em memória porque depende de JOIN
    if insight_filter != "Todos":
        _ins_counts: dict[str, int] = {}
        try:
            _ic_rows = db.query(
                "SELECT politico_id, COUNT(*) as c FROM insights "
                "WHERE politico_id IS NOT NULL AND politico_id != '' "
                "GROUP BY politico_id"
            )
            for _r in _ic_rows:
                _ins_counts[_r["politico_id"]] = _r["c"]
        except Exception:
            pass
        if insight_filter == "Com Insights":
            politicos = [p for p in politicos if _ins_counts.get(p["id"], 0) > 0]
        else:
            politicos = [p for p in politicos if _ins_counts.get(p["id"], 0) == 0]

    if not politicos:
        st.info("Nenhum político encontrado. O sistema está coletando dados...")
    else:
        # --- Paginação ---
        PER_PAGE = 50
        total_pols = len(politicos)
        total_pages = max(1, (total_pols + PER_PAGE - 1) // PER_PAGE)

        pg_col1, pg_col2, pg_col3 = st.columns([1, 2, 1])
        with pg_col2:
            page = st.number_input(
                "Página", min_value=1, max_value=total_pages, value=1, step=1,
                label_visibility="collapsed",
            )

        start_idx = (page - 1) * PER_PAGE
        end_idx = min(start_idx + PER_PAGE, total_pols)
        page_pols = politicos[start_idx:end_idx]

        # Gerar botões de página
        page_nums = []
        if total_pages <= 9:
            page_nums = list(range(1, total_pages + 1))
        else:
            page_nums = [1, 2, 3]
            if page > 4:
                page_nums.append("...")
            mid = [p for p in range(max(4, page - 1), min(total_pages - 2, page + 2)) if p not in page_nums]
            page_nums.extend(mid)
            if page < total_pages - 3:
                page_nums.append("...")
            page_nums.extend([total_pages - 1, total_pages])
            page_nums = list(dict.fromkeys(page_nums))  # deduplica mantendo ordem

        nav_html = '<div style="display:flex;justify-content:center;gap:6px;margin-bottom:14px;">'
        for pn in page_nums:
            if pn == "...":
                nav_html += '<span style="color:#8892a8;padding:6px 4px;font-size:12px;">…</span>'
            else:
                active = 'background:rgba(255,107,53,0.2);color:#ff6b35;border-color:#ff6b35;' if pn == page else ''
                nav_html += (
                    f'<span style="padding:5px 10px;border-radius:6px;font-size:12px;'
                    f'font-weight:600;border:1px solid rgba(60,70,120,0.3);color:#e0e4ef;{active}">{pn}</span>'
                )
        nav_html += '</div>'
        st.markdown(nav_html, unsafe_allow_html=True)

        table_html = (
            '<div class="table-scroll-container">'
            '<table class="ranking-table"><thead><tr>'
            '<th>NOME</th><th>PARTIDO</th><th>UF</th><th>CARGO</th><th>INSIGHTS</th>'
            '</tr></thead><tbody>'
        )

        insight_counts: dict[str, int] = {}
        try:
            rows = db.query(
                "SELECT politico_id, COUNT(*) as c FROM insights "
                "WHERE politico_id IS NOT NULL AND politico_id != '' "
                "GROUP BY politico_id"
            )
            for r in rows:
                insight_counts[r["politico_id"]] = r["c"]
        except Exception:
            pass

        for pol in page_pols:
            n_ins = insight_counts.get(pol["id"], 0)
            row_color = "color:#ff6b35;font-weight:700;" if n_ins > 0 else ""

            nome = pol["nome"]
            partido = pol.get("partido", "")
            uf = pol.get("uf", "")
            cargo = pol.get("cargo", "")
            ins_text = str(n_ins) if n_ins else "—"

            table_html += (
                f'<tr><td style="font-weight:600;">{nome}</td>'
                f'<td>{partido}</td><td>{uf}</td><td>{cargo}</td>'
                f"<td style=\"font-family:'JetBrains Mono';{row_color}\">{ins_text}</td></tr>"
            )

        table_html += '</tbody></table></div>'
        st.markdown(table_html, unsafe_allow_html=True)
        st.caption(f"Página {page} de {total_pages} — {total_pols} políticos encontrados")


def _render_tab_scanner(db: DatabaseManager):
    """Aba SCANNER: sistema autônomo + base de dados — layout premium."""
    sched = get_scheduler()
    status = sched.status
    is_running = status.get("running", False)
    current = status.get("current_task")
    uptime = sched.get_uptime()
    scans = status.get("scan_count", 0)
    errors = status.get("error_count", 0)

    status_text = f"EXECUTANDO: {current}" if current else ("ATIVO" if is_running else "INATIVO")
    border_color = "#22c55e" if is_running else "#ff2d2d"
    status_icon = "⚡" if current else ("🟢" if is_running else "🔴")

    # --- Banner de status principal ---
    st.markdown(
        f'<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;'
        f'padding:24px 28px;margin-bottom:20px;border-left:4px solid {border_color};position:relative;overflow:hidden;'
        f'transition:all 0.25s ease;" onmouseover="this.style.boxShadow=\'0 4px 20px rgba(0,0,0,0.3)\'" onmouseout="this.style.boxShadow=\'none\'">'
        f'<div style="position:absolute;top:0;right:0;width:200px;height:100%;'
        f'background:linear-gradient(90deg,transparent,rgba({"34,197,94" if is_running else "255,45,45"},0.03));pointer-events:none;"></div>'
        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
        f'<div>'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:2px;color:{border_color};margin-bottom:6px;">STATUS DO SISTEMA</div>'
        f'<div style="font-size:22px;font-weight:800;color:#ffffff;">{status_icon} {status_text}</div>'
        f'</div>'
        f'<div style="display:flex;gap:32px;">'
        f'<div style="text-align:center;">'
        f'<div style="font-family:JetBrains Mono;font-size:20px;font-weight:800;color:#ff6b35;">{uptime}</div>'
        f'<div style="font-size:9px;font-weight:600;letter-spacing:2px;color:#b0b8cc;">UPTIME</div></div>'
        f'<div style="text-align:center;">'
        f'<div style="font-family:JetBrains Mono;font-size:20px;font-weight:800;color:#3b82f6;">{scans}</div>'
        f'<div style="font-size:9px;font-weight:600;letter-spacing:2px;color:#b0b8cc;">VARREDURAS</div></div>'
        f'<div style="text-align:center;">'
        f'<div style="font-family:JetBrains Mono;font-size:20px;font-weight:800;color:{"#ff2d2d" if errors > 0 else "#22c55e"};">{errors}</div>'
        f'<div style="font-size:9px;font-weight:600;letter-spacing:2px;color:#b0b8cc;">ERROS</div></div>'
        f'</div></div></div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div style="margin-top:10px;"></div>', unsafe_allow_html=True)

    # --- Última Varredura ---
    st.markdown('<div class="section-header">Última Varredura</div>', unsafe_allow_html=True)
    try:
        varreduras = db.query("SELECT * FROM varreduras ORDER BY inicio DESC LIMIT 1")
    except Exception:
        varreduras = []

    if varreduras:
        v = varreduras[0]
        status_str = v.get("status", "")
        # Detecta varreduras travadas (em_andamento > 1h sem fim)
        is_stale = False
        if status_str == "em_andamento":
            try:
                from datetime import timedelta
                scan_start = datetime.fromisoformat(v.get("inicio", ""))
                if datetime.now() - scan_start > timedelta(hours=1):
                    is_stale = True
            except Exception:
                pass

        if status_str == "concluido":
            icon = "✅"
            badge_style = "background:rgba(34,197,94,0.1);color:#22c55e;border:1px solid rgba(34,197,94,0.3);"
            badge_text = "CONCLUÍDO"
        elif status_str == "erro":
            icon = "❌"
            badge_style = "background:rgba(255,45,45,0.1);color:#ff2d2d;border:1px solid rgba(255,45,45,0.3);"
            badge_text = "ERRO"
        elif status_str == "interrompido" or is_stale:
            icon = "⚠️"
            badge_style = "background:rgba(255,140,0,0.1);color:#ff8c00;border:1px solid rgba(255,140,0,0.3);"
            badge_text = "INTERROMPIDO"
        else:
            icon = "⏳"
            badge_style = "background:rgba(255,184,0,0.1);color:#ffb800;border:1px solid rgba(255,184,0,0.3);"
            badge_text = status_str.upper().replace("_", " ") if status_str else "PENDENTE"

        inicio = v.get("inicio", "")[:19]
        n_pol = v.get("total_politicos", 0)
        n_ins = v.get("total_insights", 0)

        st.markdown(
            f'<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;'
            f'padding:12px 16px;display:flex;justify-content:space-between;align-items:center;'
            f'transition:all 0.25s ease;" onmouseover="this.style.boxShadow=\'0 4px 20px rgba(0,0,0,0.3)\'" onmouseout="this.style.boxShadow=\'none\'">'
            f'<div style="display:flex;align-items:center;gap:10px;">'
            f'<span style="font-size:16px;">{icon}</span>'
            f'<div>'
            f'<div style="font-family:JetBrains Mono;font-size:12px;color:#ffffff;">{inicio}</div>'
            f'<div style="font-size:11px;color:#b0b8cc;margin-top:2px;">'
            f'{n_pol} políticos &middot; {n_ins} insights</div>'
            f'</div></div>'
            f'<span style="{badge_style}padding:3px 10px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:1px;">'
            f'{badge_text}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;'
            'padding:40px;text-align:center;">'
            '<div style="font-size:32px;margin-bottom:8px;">⏳</div>'
            '<div style="font-size:14px;font-weight:600;color:#b0b8cc;">Aguardando primeira varredura...</div>'
            '<div style="font-size:12px;color:#8892a8;margin-top:4px;">O sistema iniciará automaticamente</div>'
            '</div>',
            unsafe_allow_html=True,
        )

    # --- Auditoria Interna ---
    st.markdown('<div class="section-header">Auditoria Interna</div>', unsafe_allow_html=True)

    audit_fixes = status.get("audit_issues_fixed", 0)
    last_audit = status.get("last_audit")
    last_audit_fmt = last_audit[:19] if last_audit else "—"

    st.markdown(
        f'<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;'
        f'padding:20px 24px;margin-bottom:12px;border-left:4px solid #8b5cf6;position:relative;overflow:hidden;'
        f'transition:all 0.25s ease;" onmouseover="this.style.boxShadow=\'0 4px 20px rgba(0,0,0,0.3)\'" onmouseout="this.style.boxShadow=\'none\'">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
        f'<div>'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:2px;color:#8b5cf6;margin-bottom:4px;">AUDITOR AUTÔNOMO</div>'
        f'<div style="font-size:14px;font-weight:600;color:#ffffff;">Verificação contínua de integridade</div>'
        f'<div style="font-size:11px;color:#b0b8cc;margin-top:4px;">Ciclo a cada 10 min — CPF/CNPJ, valores, duplicatas, referências</div>'
        f'</div>'
        f'<div style="display:flex;gap:24px;">'
        f'<div style="text-align:center;">'
        f'<div style="font-family:JetBrains Mono;font-size:18px;font-weight:800;color:#8b5cf6;">{audit_fixes}</div>'
        f'<div style="font-size:9px;font-weight:600;letter-spacing:1px;color:#b0b8cc;">ITENS CORRIGIDOS</div></div>'
        f'<div style="text-align:center;">'
        f'<div style="font-family:JetBrains Mono;font-size:12px;font-weight:600;color:#b0b8cc;">{last_audit_fmt}</div>'
        f'<div style="font-size:9px;font-weight:600;letter-spacing:1px;color:#b0b8cc;">ÚLTIMA EXEC.</div></div>'
        f'</div></div></div>',
        unsafe_allow_html=True,
    )


def _render_tab_fontes():
    """Aba FONTES: status de todos os módulos ETL."""
    st.markdown('<div class="section-header">Fontes de Dados</div>', unsafe_allow_html=True)
    try:
        from horus.etl.registry import get_registry, ETLStatus

        registry = get_registry()
        integrados = [e for e in registry if e.status == ETLStatus.INTEGRADO]
        ativos = [e for e in registry if e.status == ETLStatus.ATIVO]
        inativos = [e for e in registry if e.status == ETLStatus.INATIVO]

        c1, c2, c3 = st.columns(3)
        c1.metric("🟢 Integradas", len(integrados))
        c2.metric("🔵 Ativas", len(ativos))
        c3.metric("🔴 Inativas", len(inativos))

        def _build_rows(entries, color):
            rows = []
            for e in entries:
                last = e.ultima_execucao or "—"
                recs = f"{e.registros_coletados:,}" if e.registros_coletados else "—"
                err = e.ultimo_erro or ""
                rows.append({
                    "Status": color,
                    "Módulo": e.nome,
                    "Fonte": e.descricao,
                    "Última Exec.": last,
                    "Registros": recs,
                    "Erro": err[:80],
                })
            return rows

        all_rows = (
            _build_rows(integrados, "🟢 Integrada")
            + _build_rows(ativos, "🔵 Ativa")
            + _build_rows(inativos, "🔴 Inativa")
        )

        import pandas as pd
        df = pd.DataFrame(all_rows)
        st.dataframe(df, use_container_width=True, hide_index=True, height=600)
    except Exception as exc:
        st.error(f"Erro ao carregar registry: {exc}")


def _render_tab_database(db: DatabaseManager):
    """Aba BASE DE DADOS: estatísticas das tabelas do banco."""
    st.markdown('<div class="section-header">Base de Dados</div>', unsafe_allow_html=True)
    try:
        db_stats = db.estatisticas()
        total_records = sum(db_stats.values())
        # Sumário no topo
        st.markdown(
            f'<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;'
            f'padding:16px;margin-bottom:12px;text-align:center;">'
            f'<div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#b0b8cc;">TOTAL DE REGISTROS</div>'
            f'<div style="font-family:JetBrains Mono;font-size:28px;font-weight:800;color:#ff6b35;">{total_records:,}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        for table, count in db_stats.items():
            label = table.replace("_", " ").title()
            color = "#ff6b35" if count > 0 else "#555"
            # Barra proporcional
            pct = min(100, int((count / max(total_records, 1)) * 100)) if count > 0 else 0
            st.markdown(
                f'<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;'
                f'padding:10px 14px;margin-bottom:6px;transition:all 0.25s ease;"'
                f' onmouseover="this.style.boxShadow=\'0 4px 20px rgba(0,0,0,0.3)\'" onmouseout="this.style.boxShadow=\'none\'">'
                f'<div style="display:flex;justify-content:space-between;margin-bottom:4px;">'
                f'<span style="font-size:12px;color:#e0e4ef;">{label}</span>'
                f'<span style="font-family:JetBrains Mono;font-weight:700;color:{color};font-size:13px;">{count:,}</span>'
                f'</div>'
                f'<div style="height:3px;background:rgba(60,70,120,0.2);border-radius:2px;overflow:hidden;">'
                f'<div style="height:100%;width:{pct}%;background:{color};border-radius:2px;"></div>'
                f'</div></div>',
                unsafe_allow_html=True,
            )
    except Exception:
        st.info("Erro ao carregar estatísticas.")


def _render_ranking_table(top_pols: list):
    # Filtrar entradas sem nome
    top_pols = [p for p in top_pols if p.get("politico_nome", "").strip()]
    if not top_pols:
        st.markdown(
            '<div class="card" style="text-align:center;padding:20px;color:#b0b8cc;">'
            'Nenhum insight vinculado a político ainda.'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    table_html = (
        '<table class="ranking-table"><thead><tr>'
        '<th>#</th><th>POLÍTICO</th><th>INSIGHTS</th><th>EXPOSIÇÃO</th><th>SCORE</th>'
        '</tr></thead><tbody>'
    )
    for i, pol in enumerate(top_pols[:10], 1):
        exp_fmt = formatar_valor(pol.get("exposicao_total", 0))
        score = pol.get("max_score", 0)
        if score >= 90:
            score_color = COLORS["CRITICO"]
        elif score >= 70:
            score_color = COLORS["ALTO"]
        else:
            score_color = COLORS["MEDIO"]

        nome = pol.get("politico_nome", "")[:30]
        n = pol.get("total_insights", 0)
        table_html += (
            f'<tr><td style="color:#b0b8cc;">{i}</td>'
            f'<td style="font-weight:600;">{nome}</td>'
            f'<td>{n}</td>'
            f"<td style=\"font-family:'JetBrains Mono';color:{score_color};\">{exp_fmt}</td>"
            f"<td style=\"font-family:'JetBrains Mono';color:{score_color};\">{score:.0f}%</td>"
            f'</tr>'
        )
    table_html += '</tbody></table>'
    st.markdown(table_html, unsafe_allow_html=True)


# =====================================================================
# MAIN
# =====================================================================

def main():
    # Garante que o scheduler está rodando (singleton)
    sched = get_scheduler()

    page_overview()

    st.markdown("---")
    st.markdown(
        '<div style="text-align:center; padding:20px; color:rgba(255,255,255,0.6); font-size:15px; line-height:1.6;">'
        '⚠ Esta análise é baseada exclusivamente em dados públicos abertos. '
        'Indica APENAS padrões estatísticos de risco e NÃO constitui prova '
        'de irregularidade ou crime. Qualquer uso deve ser validado por '
        'profissionais e pelos órgãos competentes.'
        '</div>',
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    main()
