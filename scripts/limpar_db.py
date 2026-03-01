"""Limpa dados malformados e varreduras stale do banco."""
import sqlite3
import os

db = os.path.join(os.path.dirname(__file__), "..", "data", "raiox.db")
conn = sqlite3.connect(db)
c = conn.cursor()

# Limpar varreduras estagnadas
c.execute("DELETE FROM varreduras WHERE status = 'em_andamento'")
print(f"Varreduras stale removidas: {c.rowcount}")

# Limpar contratos com dados malformados (valor=0 e cnpj vazio)
c.execute("DELETE FROM contratos WHERE fornecedor_cnpj = '' AND valor = 0.0")
print(f"Contratos malformados removidos: {c.rowcount}")

# Limpar emendas com valor 0
c.execute("DELETE FROM emendas WHERE valor_empenhado = 0.0 AND valor_pago = 0.0")
print(f"Emendas com valor 0 removidas: {c.rowcount}")

# Limpar insights antigos (gerados com dados ruins)
c.execute("DELETE FROM insights")
print(f"Insights antigos removidos: {c.rowcount}")

conn.commit()

# Verificar estado atual
for t in ["politicos", "contratos", "emendas", "sancoes", "doacoes", "despesas", "insights", "varreduras"]:
    try:
        cnt = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {cnt}")
    except Exception:
        print(f"  {t}: tabela nao existe")

conn.close()
print("DB limpo!")
