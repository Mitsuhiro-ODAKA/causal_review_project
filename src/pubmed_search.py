# src/pubmed_search.py

from Bio import Entrez
from Bio import Medline
import pandas as pd
from tqdm import tqdm
import time
import os

Entrez.email = "********@gmail.com" 

# PubMed検索クエリ（少なくとも2テーマ含む）
query = """
(
  (
    ("climate change"[MeSH Terms] OR "global warming"[Title/Abstract] OR "climate variability"[Title/Abstract] OR "extreme weather"[Title/Abstract]) AND
    ("poverty"[MeSH Terms] OR "socioeconomic factors"[MeSH Terms] OR "low-income"[Title/Abstract] OR "inequality"[Title/Abstract] OR "deprivation"[Title/Abstract]) AND
    ("malaria"[MeSH Terms] OR "tuberculosis"[MeSH Terms] OR "HIV Infections"[MeSH Terms] OR "Acquired Immunodeficiency Syndrome"[MeSH Terms] OR
     "Chagas Disease"[MeSH Terms] OR "Dengue"[MeSH Terms] OR "Leishmaniasis"[MeSH Terms] OR "Leprosy"[MeSH Terms] OR
     "Onchocerciasis"[MeSH Terms] OR "Schistosomiasis"[MeSH Terms] OR "Soil-Transmitted Helminthiasis"[Title/Abstract] OR
     "Trachoma"[MeSH Terms] OR "Lymphatic Filariasis"[MeSH Terms])
  ) OR (
    ("climate change"[MeSH Terms] OR "global warming"[Title/Abstract] OR "climate variability"[Title/Abstract] OR "extreme weather"[Title/Abstract]) AND
    ("poverty"[MeSH Terms] OR "socioeconomic factors"[MeSH Terms] OR "low-income"[Title/Abstract] OR "inequality"[Title/Abstract] OR "deprivation"[Title/Abstract])
  ) OR (
    ("climate change"[MeSH Terms] OR "global warming"[Title/Abstract] OR "climate variability"[Title/Abstract] OR "extreme weather"[Title/Abstract]) AND
    ("malaria"[MeSH Terms] OR "tuberculosis"[MeSH Terms] OR "HIV Infections"[MeSH Terms] OR "Acquired Immunodeficiency Syndrome"[MeSH Terms] OR
     "Chagas Disease"[MeSH Terms] OR "Dengue"[MeSH Terms] OR "Leishmaniasis"[MeSH Terms] OR "Leprosy"[MeSH Terms] OR
     "Onchocerciasis"[MeSH Terms] OR "Schistosomiasis"[MeSH Terms] OR "Soil-Transmitted Helminthiasis"[Title/Abstract] OR
     "Trachoma"[MeSH Terms] OR "Lymphatic Filariasis"[MeSH Terms])
  ) OR (
    ("poverty"[MeSH Terms] OR "socioeconomic factors"[MeSH Terms] OR "low-income"[Title/Abstract] OR "inequality"[Title/Abstract] OR "deprivation"[Title/Abstract]) AND
    ("malaria"[MeSH Terms] OR "tuberculosis"[MeSH Terms] OR "HIV Infections"[MeSH Terms] OR "Acquired Immunodeficiency Syndrome"[MeSH Terms] OR
     "Chagas Disease"[MeSH Terms] OR "Dengue"[MeSH Terms] OR "Leishmaniasis"[MeSH Terms] OR "Leprosy"[MeSH Terms] OR
     "Onchocerciasis"[MeSH Terms] OR "Schistosomiasis"[MeSH Terms] OR "Soil-Transmitted Helminthiasis"[Title/Abstract] OR
     "Trachoma"[MeSH Terms] OR "Lymphatic Filariasis"[MeSH Terms])
  )
)
AND
("causal inference"[Title/Abstract] OR "causality"[MeSH Terms] OR "causal relationship"[Title/Abstract] OR "causal effect"[Title/Abstract] OR
 "structural equation model"[Title/Abstract] OR "path analysis"[Title/Abstract] OR "instrumental variable"[Title/Abstract] OR
 "natural experiment"[Title/Abstract] OR "difference-in-difference"[Title/Abstract] OR "Granger causality"[Title/Abstract])
"""


# 保存先ディレクトリ
os.makedirs("../data/metadata", exist_ok=True)

# 検索してPMID一覧を取得
handle = Entrez.esearch(db="pubmed", term=query, retmax=500)
record = Entrez.read(handle)
pmid_list = record["IdList"]
print(f"Retrieved {len(pmid_list)} PMIDs")

# 論文情報の詳細を取得
records = []
batch_size = 200
for start in tqdm(range(0, len(pmid_list), batch_size), desc="Fetching PubMed records"):
    end = min(start + batch_size, len(pmid_list))
    ids = ",".join(pmid_list[start:end])
    handle = Entrez.efetch(db="pubmed", id=ids, rettype="medline", retmode="text")
    fetched = list(Medline.parse(handle))
    records.extend(fetched)
    time.sleep(0.4)  # 過負荷防止

# CSV出力用に整形
data = []
for rec in records:
    data.append({
        "title": rec.get("TI", ""),
        "authors": "; ".join(rec.get("AU", [])),
        "journal": rec.get("JT", ""),
        "year": rec.get("DP", "")[:4],
        "doi": rec.get("LID", "").split(" ")[0] if "LID" in rec else "",
        "abstract": rec.get("AB", ""),
        "pmid": rec.get("PMID", "")
    })

df = pd.DataFrame(data)
df.to_csv("../data/metadata/search_results_pubmed.csv", index=False, encoding="utf-8-sig")
print("Saved: ../data/metadata/search_results_pubmed.csv")
