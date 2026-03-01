# 🔍 HORUS

Sistema open-source de análise de risco em dados públicos brasileiros

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

---

## Visão Geral

O **HORUS** é um sistema forense de dados que cruza dezenas de bases públicas brasileiras para construir um **grafo de conhecimento** e calcular **scores de risco neutros (0–100)** para agentes públicos e empresas.

> ⚠️ **IMPORTANTE**: Este sistema **NÃO acusa ninguém de crime**. Ele calcula apenas padrões estatísticos de risco baseados em indicadores objetivos extraídos de dados públicos abertos.

---

## Funcionalidades

- **ETL Automatizado**: Coleta dados de 15+ fontes públicas brasileiras (Portal da Transparência, Receita Federal, TSE, PNCP, BCB, CVM, IBGE, etc.)
- **Grafo de Conhecimento**: Constrói rede de relacionamentos entre pessoas, empresas, contratos, emendas e sanções usando NetworkX
- **Motor de Scoring**: 25+ indicadores ponderados com explicação transparente
- **Dashboard Interativo**: Interface web moderna em Streamlit com tema dark
- **CLI Completa**: Interface de linha de comando para análises e atualizações
- **Relatórios**: Exportação em Markdown, JSON e HTML com grafo visual
- **Cache Inteligente**: Só atualiza dados quando o cache expira (padrão 30 dias)
- **Paralelismo**: ETL com ThreadPoolExecutor para downloads concorrentes
- **Retry Robusto**: Tenacity com backoff exponencial para APIs instáveis

---

## Fontes de Dados Suportadas

| # | Fonte | Tipo de Acesso | Status |
| --- | --- | --- | --- |
| 1 | Portal da Transparência (CGU) | API REST (token gratuito) | ✅ |
| 2 | CEIS/CNEP/CEAF/CEPIM (Sanções) | API REST | ✅ |
| 3 | Receita Federal CNPJ/QSA | Download ZIP mensal | ✅ |
| 4 | TSE (Candidaturas/Bens/Doações) | Download CSV | ✅ |
| 5 | PNCP (Contratos/Licitações) | API REST | ✅ |
| 6 | BCB (Selic/PTAX/PIX) | API OData | ✅ |
| 7 | CVM (Cias Abertas/Fundos) | Download CSV | ✅ |
| 8 | IBGE (IPCA/PIB/Censo) | API SIDRA | ✅ |
| 9 | SICONFI (Finanças Públicas) | API REST | ✅ |
| 10 | IPEAData | API OData | ✅ |
| 11 | Querido Diário | API REST | ✅ |
| 12 | DOU (Diário Oficial da União) | Web scraping | ✅ |
| 13 | DATASUS (SIH/SIM/CNES) | Download FTP | ✅ |
| 14 | ANVISA | Download CSV | ✅ |
| 15 | ANEEL | Portal CKAN | ✅ |

---

## Instalação

```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/HORUS.git
cd HORUS

# Criar ambiente virtual
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Instalar dependências
pip install -e .

# Configurar variáveis de ambiente
copy .env.example .env
# Edite .env e adicione seu token do Portal da Transparência
```

### Obtendo o Token do Portal da Transparência

1. Acesse: <https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email>
2. Cadastre seu email
3. Copie o token recebido para o arquivo `.env`

---

## Uso

### Via CLI

```bash
# Analisar por CPF
horus analise --cpf 12345678900

# Analisar por nome
horus analise --nome "Fulano de Tal"

# Analisar empresa por CNPJ
horus analise --cnpj 12345678000100

# Atualizar base de dados
horus atualizar --fonte transparencia
horus atualizar --todas

# Exportar relatório
horus exportar --cpf 12345678900 --formato html

# Exportar grafo
horus grafo --cpf 12345678900 --profundidade 2
```

### Via Dashboard Web

```bash
streamlit run horus/web.py
```

### Via Python

```python
from horus.config import Config
from horus.database import DatabaseManager
from horus.risk_engine import RiskEngine

config = Config()
db = DatabaseManager(config)
engine = RiskEngine(db, config)

report = engine.calcular_risco_cpf("12345678900")
print(report.to_markdown())
```

---

## Metodologia de Scoring

O score geral (0–100) é calculado como média ponderada de indicadores parciais:

| Indicador | Peso | Descrição |
| --- | --- | --- |
| Sanções ativas | 20 | CEIS/CNEP/CEAF/CEPIM |
| Empresa familiar contratada | 15 | Sócio com mesmo sobrenome + contrato público |
| Concentração de contratos | 12 | >40% contratos do órgão para 1 CNPJ |
| Doador contratado | 12 | CNPJ doou campanha e depois recebeu contrato |
| Empresa recém-criada | 10 | CNPJ <2 anos ao ganhar licitação |
| Mesmo endereço | 10 | ≥3 CNPJs no mesmo endereço |
| Emenda autodirecionada | 8 | >50% emendas para domicílio eleitoral |
| Variação patrimonial | 8 | Bens TSE crescem >300% em 1 mandato |
| Inexigibilidade alta | 7 | Valor inexigibilidade >R$500k |
| Acumulação cargos | 5 | Servidor em >1 órgão |
| (+ outros indicadores) | var. | ... |

**Níveis**: Baixo (0–25) · Médio (26–50) · Alto (51–75) · Muito Alto (76–100)

---

## Ética e Responsabilidade

- ✅ Usa **APENAS** dados públicos abertos e gratuitos
- ✅ **NUNCA** acusa ninguém de crime
- ✅ Apresenta apenas **padrões estatísticos de risco**
- ✅ Toda análise inclui disclaimer obrigatório
- ❌ **NÃO** faz vigilância ilegal
- ❌ **NÃO** usa dados privados ou protegidos

---

## Licença

Este projeto é distribuído sob a licença MIT. Veja [LICENSE](LICENSE).

---

## Disclaimer

> Esta é uma análise baseada exclusivamente em dados públicos abertos. Ela indica APENAS padrões estatísticos de risco e NÃO constitui prova de irregularidade ou crime. Qualquer uso deve ser validado por profissionais e órgãos competentes.
