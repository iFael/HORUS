"""Completa coletas pendentes: órgãos restantes, sanções, e emendas de anos anteriores."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raiox.config import Config
from raiox.database import DatabaseManager

config = Config()
db = DatabaseManager(config)

# ── 1. Contratos dos órgãos que faltaram (52000=Defesa, 20000=Presidência) ──
print("=" * 50)
print("1. CONTRATOS restantes...")
from raiox.etl.transparencia import TransparenciaETL
transp = TransparenciaETL(db)

for cod in ["52000", "20000"]:
    try:
        print(f"  Buscando contratos do órgão {cod}...")
        raw = transp.extract_contratos(codigo_orgao=cod)
        if raw:
            transformed = transp.transform({"contratos": raw})
            if "contratos" in transformed and not transformed["contratos"].empty:
                n = db.upsert_df("contratos", transformed["contratos"])
                print(f"  → {n} contratos")
    except Exception as e:
        print(f"  Erro órgão {cod}: {e}")

# ── 2. Sanções CGU ──
print("\n2. SANÇÕES CGU...")
from raiox.etl.cgu_sancoes import SancoesETL
sancoes = SancoesETL(db)
try:
    raw = sancoes.extract()
    df = sancoes.transform(raw)
    if not df.empty:
        n = sancoes.load(df)
        print(f"  → {n} sanções carregadas")
    else:
        print("  → 0 sanções (df vazio)")
except Exception as e:
    print(f"  Erro sanções: {e}")

# ── 3. Emendas de anos anteriores ──
print("\n3. EMENDAS (2023-2024)...")
for ano in [2023, 2024]:
    try:
        print(f"  Buscando emendas {ano}...")
        raw = transp.extract_emendas(ano=ano)
        if raw:
            transformed = transp.transform({"emendas": raw})
            if "emendas" in transformed and not transformed["emendas"].empty:
                n = db.upsert_df("emendas", transformed["emendas"])
                print(f"  → {ano}: {n} emendas")
            else:
                print(f"  → {ano}: 0 emendas (transformação vazia)")
        else:
            print(f"  → {ano}: 0 emendas (API vazia)")
    except Exception as e:
        print(f"  Erro emendas {ano}: {e}")

# ── 4. Re-rodar anomalias ──
print("\n4. ANÁLISE DE ANOMALIAS...")
from raiox.anomaly_detector import AnomalyDetector
detector = AnomalyDetector(db, config)
insights = detector.detect_all()
print(f"  → {len(insights)} insights gerados")
for i in insights[:10]:
    print(f"    [{i.severidade}] {i.tipo}: {i.descricao[:80]}")

# ── 5. Resumo final ──
print("\n" + "=" * 50)
print("RESUMO FINAL:")
for t in ["politicos", "contratos", "emendas", "sancoes", "insights"]:
    try:
        r = db.query(f"SELECT COUNT(*) as c FROM {t}")
        print(f"  {t}: {r[0]['c']}")
    except:
        print(f"  {t}: N/A")
print("=" * 50)
