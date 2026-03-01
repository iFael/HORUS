"""Diagnóstico rápido do banco de dados."""
from horus.config import Config
from horus.database import DatabaseManager

db = DatabaseManager(Config())

# 1. Contratos
rows = db.query("SELECT * FROM contratos LIMIT 2")
if rows:
    print("Colunas contratos:", list(rows[0].keys()))
    for r in rows:
        print({k: str(v)[:50] for k, v in r.items()})
else:
    print("Contratos: VAZIO")

# 2. Contratos com fornecedor
r2 = db.query(
    "SELECT COUNT(*) as c FROM contratos "
    "WHERE fornecedor_cnpj IS NOT NULL AND fornecedor_cnpj != '' AND valor > 0"
)
print(f"\nContratos com CNPJ+valor: {r2[0]['c']}")

# 3. Concentracao
r3 = db.query("""
    SELECT orgao, fornecedor_cnpj, fornecedor_nome,
           COUNT(*) as qtd, SUM(valor) as total
    FROM contratos
    WHERE fornecedor_cnpj != '' AND valor > 0
    GROUP BY orgao, fornecedor_cnpj
    HAVING qtd >= 3 AND total > 500000
    ORDER BY total DESC LIMIT 5
""")
print(f"\nConcentrações (>=3 contratos, >R$500k): {len(r3)}")
for x in r3:
    print(f"  {x['fornecedor_nome'][:40]}: {x['qtd']} contratos, R${x['total']:,.2f}")

# 4. Fracionamento
r4 = db.query("""
    SELECT orgao, fornecedor_cnpj, fornecedor_nome,
           COUNT(*) as qtd, SUM(valor) as total
    FROM contratos
    WHERE valor BETWEEN 41934 AND 59906
    GROUP BY orgao, fornecedor_cnpj
    HAVING qtd >= 3
    ORDER BY total DESC LIMIT 5
""")
print(f"\nFracionamentos (3+ contratos perto do limite): {len(r4)}")
for x in r4:
    print(f"  {x['fornecedor_nome'][:40]}: {x['qtd']} contratos, R${x['total']:,.2f}")

# 5. Emendas
r5 = db.query("SELECT COUNT(*) as c FROM emendas")
print(f"\nEmendas: {r5[0]['c']}")
r_em = db.query("SELECT * FROM emendas LIMIT 2")
if r_em:
    print("Colunas emendas:", list(r_em[0].keys()))

# 6. Sancoes
r6 = db.query("SELECT COUNT(*) as c FROM sancoes")
print(f"Sanções: {r6[0]['c']}")

# 7. Doacoes
try:
    r7 = db.query("SELECT COUNT(*) as c FROM doacoes")
    print(f"Doações: {r7[0]['c']}")
except:
    print("Doações: tabela não existe")

# 8. Despesas
try:
    r8 = db.query("SELECT COUNT(*) as c FROM despesas_parlamentares")
    print(f"Despesas parlamentares: {r8[0]['c']}")
except:
    print("Despesas: tabela não existe")

# 9. Varreduras
r9 = db.query("SELECT id, status, total_politicos, total_insights FROM varreduras ORDER BY inicio DESC LIMIT 5")
print(f"\nVarreduras:")
for v in r9:
    print(f"  {v['id']}: {v['status']} (pols={v['total_politicos']}, ins={v['total_insights']})")

# 10. Valores nos contratos
r10 = db.query("SELECT MIN(valor) as min_v, MAX(valor) as max_v, AVG(valor) as avg_v FROM contratos WHERE valor > 0")
if r10:
    print(f"\nContratos - Min: R${r10[0]['min_v']:,.2f}, Max: R${r10[0]['max_v']:,.2f}, Avg: R${r10[0]['avg_v']:,.2f}")
