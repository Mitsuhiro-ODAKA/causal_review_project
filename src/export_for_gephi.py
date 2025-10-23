# src/export_for_gephi.py

import pandas as pd

# 因果関係データ（LLM出力）
df_evidence = pd.read_csv("../outputs/evidence_table.csv")

# 論文メタデータ（分野情報を含む）
df_meta = pd.read_csv("../data/metadata/search_results_combined.csv")
df_meta["journal"] = df_meta["journal"].fillna("").str.lower().str.strip()

# 分野マッピング（簡略化）
journal_field_map = {
    "the lancet": "医学・公衆衛生",
    "plos neglected tropical diseases": "熱帯医学",
    "environmental research": "環境科学",
    "climate change economics": "気候経済学",
    "development policy review": "開発経済学",
    "health policy and planning": "グローバルヘルス政策",
    "science of the total environment": "環境科学",
    "bmc public health": "公衆衛生",
    "ecological economics": "環境経済学"
}

def map_field(journal):
    for key in journal_field_map:
        if key in journal:
            return journal_field_map[key]
    return "その他・未分類"

df_meta["field"] = df_meta["journal"].apply(map_field)

# 論文ID → 分野 のマッピング作成
paper_field_map = df_meta.set_index("id")["field"].to_dict()

# エッジリスト作成
df_evidence["source"] = df_evidence["cause"]
df_evidence["target"] = df_evidence["effect"]
df_evidence["field"] = df_evidence["paper_id"].map(paper_field_map)

edges = df_evidence[["source", "target", "field", "description", "paper_id"]].dropna()

# ノードリスト作成
nodes = pd.DataFrame(
    pd.unique(edges[["source", "target"]].values.ravel()),
    columns=["id"]
)
nodes["label"] = nodes["id"]

# 保存
edges.to_csv("../outputs/edges.csv", index=False)
nodes.to_csv("../outputs/nodes.csv", index=False)
print("Saved: outputs/nodes.csv and outputs/edges.csv")
