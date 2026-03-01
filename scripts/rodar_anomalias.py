"""Re-rodar detecção de anomalias e mostrar estado final."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from horus.config import Config
from horus.database import DatabaseManager

config = Config()
db = DatabaseManager(config)

# Contagens
print("ESTADO DO BANCO:")
for t in ["politicos", "contratos", "emendas", "sancoes", "insights"]:
    try:
        r = db.query(f"SELECT COUNT(*) as c FROM {t}")
        print(f"  {t}: {r[0]['c']}")
    except Exception as ex:
        print(f"  {t}: ERRO - {ex}")

# Contratos com dados válidos
r = db.query("SELECT COUNT(*) as c FROM contratos WHERE fornecedor_cnpj != '' AND valor > 0")
print(f"\n  Contratos com CNPJ+valor: {r[0]['c']}")

# Re-rodar anomalias
print("\nRODANDO ANOMALIAS...")
from horus.anomaly_detector import AnomalyDetector
detector = AnomalyDetector(db, config)
try:
    insights = detector.detect_all()
    print(f"  Total insights: {len(insights)}")
    for i in insights[:15]:
        desc = str(i.descricao)[:85] if i.descricao else ""
        print(f"    [{i.severidade}] {i.tipo}: {desc}")
except Exception as e:
    import traceback
    print(f"  ERRO: {e}")
    traceback.print_exc()

# Resumo final
print("\nESTADO FINAL:")
for t in ["politicos", "contratos", "emendas", "sancoes", "insights"]:
    try:
        r = db.query(f"SELECT COUNT(*) as c FROM {t}")
        print(f"  {t}: {r[0]['c']}")
    except Exception as ex:
        print(f"  {t}: ERRO - {ex}")
