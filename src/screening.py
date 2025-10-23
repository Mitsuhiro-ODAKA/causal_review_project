import pandas as pd

df = pd.read_csv("../data/metadata/search_results_combined.csv")

# 除外ワード例：非感染症、統計のみ、モデル不在
exclusion_keywords = ["regression only", "cross-sectional", "qualitative", "non-infectious", "interview"]

def check_exclusion(abstract):
    if pd.isna(abstract):
        return True
    abstract = abstract.lower()
    return any(kw in abstract for kw in exclusion_keywords)

df["excluded"] = df["abstract"].apply(check_exclusion)
df.to_csv("../data/metadata/filtered_articles.csv", index=False)
print("Saved to ../data/metadata/filtered_articles.csv")