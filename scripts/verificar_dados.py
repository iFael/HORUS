"""Verificação rápida dos dados após re-scan."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raiox.config import Config
from raiox.database import DatabaseManager

config = Config()
db = DatabaseManager(config)

print("=" * 60)
print("VERIFICAÇÃO PÓS RE-SCAN")
print("=" * 60)

# Contagens
for t in ["politicos", "contratos", "emendas", "sancoes", "doacoes", "licitacoes", "insights", "varreduras"]:
    try:
        r = db.query(f"SELECT COUNT(*) as c FROM {t}")
        print(f"  {t}: {r[0]['c']}")
    except:
        print(f"  {t}: (tabela não existe)")

# Contratos com CNPJ e valor
r = db.query("SELECT COUNT(*) as c FROM contratos WHERE fornecedor_cnpj != '' AND fornecedor_cnpj IS NOT NULL")
print(f"\nContratos com CNPJ: {r[0]['c']}")
r = db.query("SELECT COUNT(*) as c FROM contratos WHERE valor > 0")
print(f"Contratos com valor > 0: {r[0]['c']}")

# Amostra de contratos
r = db.query("SELECT fornecedor_cnpj, fornecedor_nome, valor, orgao FROM contratos WHERE valor > 0 LIMIT 5")
print("\nAmostra de contratos com valor:")
for row in r:
    print(f"  CNPJ={row['fornecedor_cnpj']}, Nome={row['fornecedor_nome'][:40]}, Valor=R${row['valor']:,.2f}, Orgao={row['orgao'][:35]}")

# Estatísticas de valor
r = db.query("SELECT MIN(valor) as mn, MAX(valor) as mx, AVG(valor) as av, SUM(valor) as sm FROM contratos WHERE valor > 0")
if r and r[0]['mx'] is not None:
    print(f"\nValores: Min=R${r[0]['mn']:,.2f}, Max=R${r[0]['mx']:,.2f}, Avg=R${r[0]['av']:,.2f}, Total=R${r[0]['sm']:,.2f}")

# Concentrações de fornecedor
r = db.query("""
    SELECT orgao, fornecedor_cnpj, fornecedor_nome, COUNT(*) as qtd, SUM(valor) as total
    FROM contratos WHERE fornecedor_cnpj != '' AND valor > 0
    GROUP BY orgao, fornecedor_cnpj HAVING qtd >= 3 AND total > 500000
    ORDER BY total DESC LIMIT 5
""")
print(f"\nConcentrações (>=3 contratos, >R$500k): {len(r)}")
for x in r:
    print(f"  {x['fornecedor_nome'][:35]} @ {x['orgao'][:30]} = {x['qtd']} contratos, R${x['total']:,.2f}")

# Emendas
r = db.query("SELECT COUNT(*) as c FROM emendas WHERE valor_empenhado > 0")
print(f"\nEmendas com valor_empenhado > 0: {r[0]['c']}")
r = db.query("SELECT autor, valor_empenhado, localidade FROM emendas WHERE valor_empenhado > 0 LIMIT 3")
for x in r:
    print(f"  {x['autor'][:40]} = R${x['valor_empenhado']:,.2f} ({x['localidade'][:25]})")

# Sancoes
r = db.query("SELECT COUNT(*) as c FROM sancoes")
print(f"\nSanções: {r[0]['c']}")

# Varredura
r = db.query("SELECT * FROM varreduras ORDER BY inicio DESC LIMIT 3")
print("\nÚltimas varreduras:")
for v in r:
    print(f"  {v['id']}: {v['status']} (pols={v.get('total_politicos',0)}, ins={v.get('total_insights',0)})")

print("\n" + "=" * 60)
