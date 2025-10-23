import pandas as pd
from collections import defaultdict

# ファイル読み込み（search_results_combined.csv でも可）
df = pd.read_csv("../outputs/evidence_table.csv")

# ジャーナル名をクリーニング
df["journal"] = df["journal"].fillna("").str.lower().str.strip()

# マッピング辞書（必要に応じて拡張）
journal_field_map = {
    "the lancet": "医学・公衆衛生",
    "plos neglected tropical diseases": "熱帯医学",
    "environmental research": "環境科学",
    "climate change economics": "気候経済学",
    "development policy review": "開発経済学",
    "health policy and planning": "グローバルヘルス政策",
    "science of the total environment": "環境科学",
    "bmc public health": "公衆衛生",
    "ecological economics": "環境経済学",
    "epidemiology and infection": "疫学"
}

# 分野列の生成
def map_field(journal):
    for key in journal_field_map:
        if key in journal:
            return journal_field_map[key]
    return "その他・未分類"

df["field"] = df["journal"].apply(map_field)

# 集計
field_counts = df["field"].value_counts().reset_index()
field_counts.columns = ["field", "count"]

# 出力
print("研究分野別の論文件数：\n")
print(field_counts)

# グラフ（任意）
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 5))
plt.barh(field_counts["field"], field_counts["count"])
plt.title("研究分野別 論文数")
plt.xlabel("件数")
plt.ylabel("研究分野")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()
