# src/crossref_search.py

import requests
import pandas as pd
from tqdm import tqdm
import time
import os

# 保存先ディレクトリ
os.makedirs("../data/metadata", exist_ok=True)

# クエリ（3テーマのうち少なくとも2つ＋因果）
query = (
    '('
    # ─── 最低 2 要素 (3 ペア＋トリプル) ───
    '('
        '('  # ① CL ∧ PV ∧ DZ
            '("climate change" OR "global warming" OR "climate variability" OR "extreme weather") AND '
            '("poverty" OR "socioeconomic status" OR "low-income" OR "inequality" OR "deprivation") AND '
            '("malaria" OR "tuberculosis" OR "HIV" OR "AIDS" OR "Chagas disease" OR "dengue" OR '
            '"leishmaniasis" OR "leprosy" OR "onchocerciasis" OR "schistosomiasis" OR '
            '"soil-transmitted helminthiases" OR "trachoma" OR "lymphatic filariasis")'
        ') OR ('
            # ② CL ∧ PV
            '("climate change" OR "global warming" OR "climate variability" OR "extreme weather") AND '
            '("poverty" OR "socioeconomic status" OR "low-income" OR "inequality" OR "deprivation")'
        ') OR ('
            # ③ CL ∧ DZ
            '("climate change" OR "global warming" OR "climate variability" OR "extreme weather") AND '
            '("malaria" OR "tuberculosis" OR "HIV" OR "AIDS" OR "Chagas disease" OR "dengue" OR '
            '"leishmaniasis" OR "leprosy" OR "onchocerciasis" OR "schistosomiasis" OR '
            '"soil-transmitted helminthiases" OR "trachoma" OR "lymphatic filariasis")'
        ') OR ('
            # ④ PV ∧ DZ
            '("poverty" OR "socioeconomic status" OR "low-income" OR "inequality" OR "deprivation") AND '
            '("malaria" OR "tuberculosis" OR "HIV" OR "AIDS" OR "Chagas disease" OR "dengue" OR '
            '"leishmaniasis" OR "leprosy" OR "onchocerciasis" OR "schistosomiasis" OR '
            '"soil-transmitted helminthiases" OR "trachoma" OR "lymphatic filariasis")'
        ')'
    ')'
    ' AND ('
        # ─── 因果キーワード ───
        '"causal inference" OR "causality" OR "causal relationship" OR "causal effect" OR '
        '"structural equation model" OR "path analysis" OR "instrumental variable" OR '
        '"natural experiment" OR "difference-in-difference" OR "Granger causality"'
    ')'
)


# URLとパラメータ
url = "https://api.crossref.org/works"
params = {
    "query": query,
    "rows": 200,  # 最大件数
    "mailto": "*******@gmail.com" 
}

# リクエスト
print("Querying Crossref...")
r = requests.get(url, params=params, headers={"User-Agent": "causal-review/1.0 (mailto:your_email@example.com)"})
data = r.json()

# 結果の抽出
items = data.get("message", {}).get("items", [])
results = []
for item in tqdm(items, desc="Processing results"):
    authors = "; ".join([f"{a.get('given', '')} {a.get('family', '')}" for a in item.get("author", [])]) if "author" in item else ""
    results.append({
        "title": item.get("title", [""])[0],
        "doi": item.get("DOI", ""),
        "authors": authors,
        "journal": item.get("container-title", [""])[0],
        "year": item.get("issued", {}).get("date-parts", [[None]])[0][0],
        "abstract": item.get("abstract", "")
    })

# 保存
df = pd.DataFrame(results)
df.to_csv("../data/metadata/search_results_crossref.csv", index=False, encoding="utf-8-sig")
print("Saved: ../data/metadata/search_results_crossref.csv")
