import requests
import pandas as pd
import time
import random
from tqdm import tqdm

# Semantic Scholar API endpoint
url = "https://api.semanticscholar.org/graph/v1/paper/search"
fields = "title,abstract,authors,year,venue,externalIds"

# 検索クエリのバリエーション（2項組み合わせと3項）
queries = [
    # 貧困 × 感染症
    '(("poverty" OR "socioeconomic status" OR "low-income" OR "inequality" OR "deprivation") AND '
    ' ("malaria" OR "tuberculosis" OR "HIV" OR "AIDS" OR "Chagas disease" OR "dengue" OR '
    ' "leishmaniasis" OR "leprosy" OR "onchocerciasis" OR "schistosomiasis" OR '
    ' "soil-transmitted helminthiases" OR "trachoma" OR "lymphatic filariasis")) AND '
    ' ("causal inference" OR "causality" OR "causal relationship" OR "causal effect" OR '
    ' "structural equation model" OR "path analysis" OR "instrumental variable" OR '
    ' "natural experiment" OR "difference-in-difference" OR "Granger causality")',

    # 気候 × 感染症
    '(("climate change" OR "global warming" OR "climate variability" OR "extreme weather") AND '
    ' ("malaria" OR "tuberculosis" OR "HIV" OR "AIDS" OR "Chagas disease" OR "dengue" OR '
    ' "leishmaniasis" OR "leprosy" OR "onchocerciasis" OR "schistosomiasis" OR '
    ' "soil-transmitted helminthiases" OR "trachoma" OR "lymphatic filariasis")) AND '
    ' ("causal inference" OR "causality" OR "causal relationship" OR "causal effect" OR '
    ' "structural equation model" OR "path analysis" OR "instrumental variable" OR '
    ' "natural experiment" OR "difference-in-difference" OR "Granger causality")',

    # 気候 × 貧困
    '(("climate change" OR "global warming" OR "climate variability" OR "extreme weather") AND '
    ' ("poverty" OR "socioeconomic status" OR "low-income" OR "inequality" OR "deprivation")) AND '
    ' ("causal inference" OR "causality" OR "causal relationship" OR "causal effect" OR '
    ' "structural equation model" OR "path analysis" OR "instrumental variable" OR '
    ' "natural experiment" OR "difference-in-difference" OR "Granger causality")',

    # すべての要素
    '(("climate change" OR "global warming" OR "climate variability" OR "extreme weather") AND '
    ' ("poverty" OR "socioeconomic status" OR "low-income" OR "inequality" OR "deprivation") AND '
    ' ("malaria" OR "tuberculosis" OR "HIV" OR "AIDS" OR "Chagas disease" OR "dengue" OR '
    ' "leishmaniasis" OR "leprosy" OR "onchocerciasis" OR "schistosomiasis" OR '
    ' "soil-transmitted helminthiases" OR "trachoma" OR "lymphatic filariasis")) AND '
    ' ("causal inference" OR "causality" OR "causal relationship" OR "causal effect" OR '
    ' "structural equation model" OR "path analysis" OR "instrumental variable" OR '
    ' "natural experiment" OR "difference-in-difference" OR "Granger causality")'
]

# 出力データ保持用
all_results = []

# クエリごとに取得処理
for q in tqdm(queries, desc="Querying Semantic Scholar"):
    for offset in range(0, 100, 25):  # 最大100件（25件ずつ）
        params = {
            "query": q,
            "limit": 25,
            "offset": offset,
            "fields": fields
        }

        for attempt in range(5):
            try:
                response = requests.get(url, params=params, headers={"Accept": "application/json"})

                if response.status_code == 200:
                    data = response.json().get("data", [])
                    for item in data:
                        doi = item.get("externalIds", {}).get("DOI", "")
                        authors = "; ".join([a.get("name", "") for a in item.get("authors", [])])
                        all_results.append({
                            "query": q,
                            "title": item.get("title", ""),
                            "doi": doi,
                            "authors": authors,
                            "year": item.get("year", ""),
                            "journal": item.get("venue", ""),
                            "abstract": item.get("abstract", "")
                        })
                    time.sleep(random.uniform(8, 12))  # クエリ間のスリープ
                    break
                elif response.status_code == 429:
                    wait = (2 ** attempt) + random.uniform(0, 2)
                    print(f"429 Rate limit hit. Sleeping {wait:.1f} sec...")
                    time.sleep(wait)
                else:
                    print(f"Error {response.status_code}: {response.text}")
                    break
            except Exception as e:
                print(f"Exception on query '{q}' (offset {offset}): {e}")
                time.sleep(10)

# 保存
df = pd.DataFrame(all_results)
df.to_csv("../data/metadata/search_results_semantic_scholar.csv", index=False, encoding="utf-8-sig")
print("Saved to: search_results_semantic_scholar.csv")
