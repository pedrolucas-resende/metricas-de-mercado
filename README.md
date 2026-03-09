# 🏍️ Métricas de Mercado — Mottu

Análise da frota de motocicletas no Brasil por município, usando dados públicos do SENATRAN/RENAVAM.

---

## 📁 Estrutura do Projeto

```
metricas-de-mercado/
│
├── download_senatran.py          # Script de download e processamento dos dados
├── analise_motos.ipynb           # Notebook com gráfico e previsão
├── README.md
│
└── senatran_csvs/
    └── janeiro/
        ├── frota_2020_01.csv
        ├── frota_2021_01.csv
        ├── frota_2022_01.csv
        ├── frota_2023_01.csv
        ├── frota_2024_01.csv
        └── frota_2025_01.csv
    └── frota_janeiro_2020_2025.csv   # CSV unificado
```

---

## 🚀 Como usar

### 1. Instale as dependências

```bash
python -m venv .venv
source .venv/bin/activate        # Mac/Linux
# .venv\Scripts\activate         # Windows

pip install requests pandas openpyxl xlrd scikit-learn matplotlib ipykernel
python -m ipykernel install --user --name=venv
```

### 2. Baixe os dados do SENATRAN

```bash
python download_senatran.py
```

Isso vai baixar os arquivos de Janeiro de 2020 a 2025 e gerar o CSV unificado em `senatran_csvs/frota_janeiro_2020_2025.csv`.

### 3. Abra o notebook

```bash
jupyter notebook
```

Selecione o kernel **venv** e rode todas as células do `analise_motos.ipynb`.

---

## 📊 Dados

| Coluna | Descrição |
|---|---|
| `MUNICIPIO` | Nome do município |
| `TOTAL` | Total de veículos registrados |
| `MOTOCICLETA` | Total de motocicletas registradas |
| `ANO` | Ano de referência |
| `MES` | Mês de referência (sempre 1 = Janeiro) |

**Fonte:** [SENATRAN / Ministério dos Transportes](https://www.gov.br/transportes/pt-br/assuntos/transito/senatran)  
**Periodicidade:** Mensal (este projeto usa apenas Janeiro de cada ano)  
**Cobertura:** 2020 a 2025

---

## 📈 Análises disponíveis

![Frota de Motocicletas no Brasil](senatran_csvs/frota_motos_previsao.png)



- Evolução da frota de motocicletas por ano (Brasil)
- Previsão para 2026 e 2027 via regressão linear
- Base pronta para calcular **% de penetração da Mottu** por município

---

## 🧮 Métrica principal

```
Penetração Mottu (%) = Clientes Mottu na cidade ÷ Frota de motos na cidade × 100
```

---

## 🔗 Fontes

- [SENATRAN — Frota de Veículos](https://www.gov.br/transportes/pt-br/assuntos/transito/senatran)
- [ABRACICLO — Emplacamentos mensais](https://www.abraciclo.org.br)
- [IBGE — Censo 2022](https://www.ibge.gov.br/censo2022)
- [CAGED — Motoboys formais](https://www.gov.br/trabalho-e-emprego)
