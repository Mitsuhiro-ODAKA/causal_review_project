import requests
import pandas as pd
import time
import os
from tqdm import tqdm

# 出力先ディレクトリ
os.makedirs("../data/metadata", exist_ok=True)

# 検索クエリ（少なくとも2テーマ＋因果関係）
query = (
    '('
    '('
        'climate change OR global warming OR climate variability OR extreme weather'
        ' AND '
        'poverty OR socioeconomic status OR low-income OR inequality OR deprivation'
    ') OR ('
        'climate change OR global warming OR climate variability OR extreme weather'
        ' AND '
        'malaria OR tuberculosis OR HIV OR AIDS OR "Chagas disease" OR dengue OR '
        'leishmaniasis OR leprosy OR onchocerciasis OR schistosomiasis OR '
        'soil-transmitted helminthiases OR trachoma OR "lymphatic filariasis"'
    ') OR ('
        'poverty OR socioeconomic status OR low-income OR inequality OR deprivation'
        ' AND '
        'malaria OR tuberculosis OR HIV OR AIDS OR "Chagas disease" OR dengue OR '
        'leishmaniasis OR leprosy OR onchocerciasis OR schistosomiasis OR '
        'soil-transmitted helminthiases OR trachoma OR "lymphatic filariasis"'
    ')'
    ') AND '
    'causal inference OR causality OR causal relationship OR causal effect OR '
    'structural equation model OR path analysis OR instrumental variable OR '
    'natural experiment OR difference-in-difference OR Granger causality'
)

base_url = "https://api.openalex.org/works"
per_page = 200
cursor = "*"
all_results = []
max_results = 500
stop = False

# ページネーション取得（最大500件まで）
while not stop:
    params = {
        "search": query,
        "per-page": per_page,
        "cursor": cursor,
        "select": "id,doi,display_name,abstract_inverted_index,publication_year,authorships,biblio"
    }
    resp = requests.get(base_url, params=params)
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        break
    data = resp.json()

    for item in data.get("results", []):
        if len(all_results) >= max_results:
            stop = True
            break

        # ジャーナル名を biblio.journal_title から取得
        biblio = item.get("biblio", {})
        journal = biblio.get("journal_title", "")

        # 抽象文を復元
        inv_idx = item.get("abstract_inverted_index", {})
        if inv_idx:
            flat = [(pos, term) for term, positions in inv_idx.items() for pos in positions]
            abstract = " ".join(term for _, term in sorted(flat))
        else:
            abstract = ""

        # 著者名
        authors = "; ".join([a.get("author", {}).get("display_name", "") for a in item.get("authorships", [])])

        all_results.append({
            "title": item.get("display_name", ""),
            "doi": item.get("doi", ""),
            "authors": authors,
            "year": item.get("publication_year", ""),
            "journal": journal,
            "abstract": abstract,
            "openalex_id": item.get("id", ""),
            "url": item.get("id", "")
        })

    if stop:
        break

    # 次のカーソル確認
    next_cursor = data.get("meta", {}).get("next_cursor")
    if not next_cursor or next_cursor == cursor:
        break
    cursor = next_cursor
    time.sleep(1)

# 結果を1000件にトリム
all_results = all_results[:max_results]

# DataFrame化＆保存
df = pd.DataFrame(all_results)
df.to_csv("../data/metadata/search_results_openalex.csv", index=False, encoding="utf-8-sig")
print(f"Saved {len(df)} records to: ../data/metadata/search_results_openalex.csv")
