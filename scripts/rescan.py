"""Script para re-popular o banco com os ETLs corrigidos."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raiox.config import Config
from raiox.database import DatabaseManager
from raiox.scanner import PoliticianScanner

print("=" * 60)
print("ORUS — Re-scan com ETLs corrigidos")
print("=" * 60)

config = Config()
db = DatabaseManager(config)
scanner = PoliticianScanner(db, config)

def progress(etapa, detalhe):
    print(f"  [{etapa}] {detalhe}")

resultado = scanner.scan_all(skip_despesas=True, progress_callback=progress)

print("\n" + "=" * 60)
print("RESULTADO:")
for k, v in resultado.get("etapas", {}).items():
    print(f"  {k}: {v}")
print(f"  Status: {resultado.get('status')}")
if resultado.get("erro"):
    print(f"  ERRO: {resultado['erro']}")
print(f"  Duração: {resultado.get('duracao_s', 0):.1f}s")
print("=" * 60)
