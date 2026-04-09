# ReviCX Health Score

Sistema de Health Score para Customer Success da ReviCX. Cruza dados do produto (ReviCX) com dados do CRM (HubSpot), centralizado em um data warehouse (Nekt).

**Fase atual: Mock** — dados ficticios para validar logica, dashboards e alertas.

## Stack

- Python 3.11+
- SQLite (simula o warehouse Nekt)
- Streamlit (dashboards)
- Plotly (graficos)
- Pandas + SQLAlchemy

## Como rodar

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Gerar dados mockados
python scripts/01_generate_mock_data.py

# 3. Criar tabelas dimensao
python scripts/02_create_dimensions.py

# 4. Calcular Health Score
python scripts/03_calculate_health_score.py

# 5. Gerar alertas
python scripts/04_generate_alerts.py

# 6. Backfill de 6 meses de historico
python scripts/05_backfill_history.py

# 7. Rodar o dashboard
streamlit run dashboard/app.py
```

## Estrutura

```
revi-cs-health-score/
├── config/scoring_rules.yaml   # Regras do Health Score (editavel sem codigo)
├── data/revi_cs.db             # SQLite gerado pelos scripts
├── scripts/                    # Pipeline de dados
├── dashboard/app.py            # Dashboard Streamlit
└── sql/                        # Queries SQL portaveis para o Nekt
```

## Health Score

6 criterios, 5 pontos cada (max 30):

| Criterio | 5 pts | 3-4 pts | 0 pts |
|----------|-------|---------|-------|
| Recencia | <=7 dias | 8-20 dias | >20 dias |
| ROI | >=10x | 1-10x | <1x |
| Automacoes | 3+ | 1-2 | 0 |
| Integracoes | 4+ | 1-3 | 0 |
| Chat | Avancado | Essencial | Nenhum |
| Volume | Crescente | Estavel | Queda |

**Classificacao:** 🟢 Campeao (26+) | 🟡 Alerta (16-25) | 🔴 Em Risco (0-15)
