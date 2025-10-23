# src/merge_search_results.py

import pandas as pd
import os

# 各CSVファイルのパス
FILES = {
    "crossref": "../data/metadata/search_results_crossref.csv",
    "semantic": "../data/metadata/search_results_semantic_scholar.csv",
    "pubmed": "../data/metadata/search_results_pubmed.csv",
    "openalex": "../data/metadata/search_results_openalex.csv"
}

# 標準カラム構造
STANDARD_COLUMNS = ["title", "doi", "authors", "year", "journal", "abstract", "url", "source"]

all_records = []

for source_name, file_path in FILES.items():
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue

    try:
        df = pd.read_csv(file_path, on_bad_lines="skip", encoding="utf-8")
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        continue

    if df.empty:
        print(f"File is empty: {file_path}")
        continue

    # 出典列を追加
    df["source"] = source_name

    # カラム名のマッピング
    column_map = {
        "Title": "title", "title": "title",
        "DOI": "doi", "doi": "doi",
        "Authors": "authors", "authors": "authors",
        "Year": "year", "year": "year",
        "Journal": "journal", "journal": "journal",
        "publicationName": "journal", "host_venue": "journal", "biblio.journal_title": "journal",
        "abstract": "abstract", "Abstract": "abstract", "abstract_inverted_index": "abstract",
        "URL": "url", "url": "url", "id": "url", "openalex_id": "url"
    }

    df = df.rename(columns=column_map)

    # 重複カラムがあれば除去（最初の出現以外を削除）
    df = df.loc[:, ~df.columns.duplicated()]

    # 標準カラムをすべて持たせる
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # 必要な順序で抽出
    df = df[STANDARD_COLUMNS]

    all_records.append(df)

# 結合チェック
if not all_records:
    print("No valid data to merge.")
    exit(1)

# マージ
df_all = pd.concat(all_records, ignore_index=True)

# 重複除去（doi優先、なければtitle）
df_all["doi"]   = df_all["doi"].fillna("").astype(str)
df_all["title"] = df_all["title"].fillna("").astype(str)
df_all = df_all.drop_duplicates(subset=["doi","title"], keep="first")

# 一意のIDを付与
df_all.insert(0, "id", ["A" + str(i+1).zfill(3) for i in range(len(df_all))])

# 欠損処理
df_all["abstract"] = df_all["abstract"].fillna("No abstract available")
df_all["year"]     = df_all["year"].fillna("")

# 保存（Excel互換のBOM付きUTF-8）
OUTPUT_FILE = "../data/metadata/search_results_combined.csv"
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
df_all.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print(f"Merged search results saved to: {OUTPUT_FILE}")
