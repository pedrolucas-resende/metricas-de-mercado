import requests
import pandas as pd
from pathlib import Path
import os

COLUNAS = ["MUNICIPIO", "TOTAL", "MOTOCICLETA"]

JANEIRO_URLS = {
    2020: ("https://www.gov.br/transportes/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/2020/janeiro/frota_munic_modelo_janeiro_2020.xls",       "xlrd"),
    2021: ("https://www.gov.br/transportes/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/2021/janeiro/frota_municipio_modelo_janeiro_2021.xlsx",  "openpyxl"),
    2022: ("https://www.gov.br/transportes/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/2022/janeiro/frota_munic_modelo_janeiro_2022.xlsx",      "openpyxl"),
    2023: ("https://www.gov.br/transportes/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/2023/janeiro/frota_munic_modelo_janeiro_2023.xls",       "xlrd"),
    2024: ("https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/FrotaMunicModelo07Janeiro2024.xls",                                           "xlrd"),
    2025: ("https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/Frota_por_municipio_tipo_Janeiro_2025.xlsx",                                  "openpyxl"),
}

output_dir = Path("senatran_csvs/janeiro")
output_dir.mkdir(parents=True, exist_ok=True)

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("🚀 Baixando Janeiro de 2020 a 2025\n")

for ano, (url, engine) in JANEIRO_URLS.items():
    nome_arquivo = f"frota_{ano}_01"
    csv_path = output_dir / f"{nome_arquivo}.csv"

    print(f"⬇️  {ano} — {url}")
    try:
        response = requests.get(url, headers=headers, timeout=120)
        print(f"   Status: {response.status_code}")

        if response.status_code != 200:
            print(f"❌  {ano} — HTTP {response.status_code}\n")
            continue

        ext = ".xls" if engine == "xlrd" else ".xlsx"
        tmp_path = output_dir / f"{nome_arquivo}{ext}"
        with open(tmp_path, "wb") as f:
            f.write(response.content)

        df = pd.read_excel(tmp_path, engine=engine, skiprows=2)
        df.dropna(how="all", inplace=True)
        df.columns = [str(c).strip().upper() for c in df.columns]
        df = df.iloc[1:].reset_index(drop=True)  # remove primeira linha

        print(f"   Colunas encontradas: {list(df.columns)}")

        df = df[COLUNAS]
        df["ANO"] = ano
        df["MES"] = 1

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        os.remove(tmp_path)

        print(f"✅  {df.shape[0]} municípios → {csv_path.name}\n")

    except Exception as e:
        print(f"❌  {ano} — Erro: {e}\n")

print("=" * 50)
print("✅ Concluído!")

# Junta todos os CSVs em um único arquivo
todos = [pd.read_csv(csv) for csv in sorted(output_dir.glob("*.csv"))]
df_final = pd.concat(todos, ignore_index=True)

final_path = Path("senatran_csvs/frota_janeiro_2020_2025.csv")
df_final.to_csv(final_path, index=False, encoding="utf-8-sig")

print(f"📊 Total de linhas: {df_final.shape[0]}")
print(f"✅ CSV unificado salvo em: {final_path}")
