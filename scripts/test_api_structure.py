"""Teste rápido: ver estrutura real da API do Portal da Transparência."""
import json
import requests

TOKEN = "42d7cca75b04941cc79127affb78dd5b"
BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"

headers = {"chave-api-dados": TOKEN, "Accept": "application/json"}

# 1. Contratos (CGU, código 26246)
print("=" * 60)
print("CONTRATOS (codigoOrgao=26246)")
print("=" * 60)
resp = requests.get(f"{BASE}/contratos", headers=headers, params={"codigoOrgao": "26246", "pagina": 1}, timeout=30)
print(f"Status: {resp.status_code}")
data = resp.json()
if isinstance(data, list) and data:
    print(f"Total itens: {len(data)}")
    print(f"Chaves: {list(data[0].keys())}")
    print(json.dumps(data[0], indent=2, ensure_ascii=False, default=str)[:2000])
else:
    print("Resposta:", str(data)[:500])

# 2. Emendas (2024)
print("\n" + "=" * 60)
print("EMENDAS (ano=2024)")
print("=" * 60)
resp2 = requests.get(f"{BASE}/emendas", headers=headers, params={"ano": 2024, "pagina": 1}, timeout=30)
print(f"Status: {resp2.status_code}")
data2 = resp2.json()
if isinstance(data2, list) and data2:
    print(f"Total itens: {len(data2)}")
    print(f"Chaves: {list(data2[0].keys())}")
    print(json.dumps(data2[0], indent=2, ensure_ascii=False, default=str)[:2000])
else:
    print("Resposta:", str(data2)[:500])

# 3. Sanções CEIS
print("\n" + "=" * 60)
print("CEIS (sanções)")
print("=" * 60)
resp3 = requests.get(f"{BASE}/ceis", headers=headers, params={"pagina": 1}, timeout=30)
print(f"Status: {resp3.status_code}")
data3 = resp3.json()
if isinstance(data3, list) and data3:
    print(f"Total itens: {len(data3)}")
    print(f"Chaves: {list(data3[0].keys())}")
    print(json.dumps(data3[0], indent=2, ensure_ascii=False, default=str)[:2000])
else:
    print("Resposta:", str(data3)[:500])
