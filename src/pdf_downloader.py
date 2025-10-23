import os
import pandas as pd
import requests
from tqdm import tqdm
from urllib.parse import quote

# 保存先
PDF_DIR = "../data/raw"
os.makedirs(PDF_DIR, exist_ok=True)

# メタデータの読み込み（'doi'列があることが前提）
df = pd.read_csv("../data/metadata/filtered_articles.csv")

# Unpaywall経由でPDF URL取得
def get_pdf_url_unpaywall(doi, email="secnilape@gmail.com"):
    api_url = f"https://api.unpaywall.org/v2/{quote(doi)}?email={email}"
    try:
        r = requests.get(api_url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            best_oa = data.get("best_oa_location")
            if best_oa:
                return best_oa.get("url_for_pdf")
        else:
            print(f"Unpaywall error {r.status_code} for DOI {doi}")
    except Exception as e:
        print(f"Exception on {doi}: {e}")
    return None

# PDFダウンロード処理
def download_pdf(pdf_url, filename):
    try:
        r = requests.get(pdf_url, timeout=20)
        if r.status_code == 200 and 'application/pdf' in r.headers.get('Content-Type', ''):
            file_path = os.path.join(PDF_DIR, filename)
            with open(file_path, "wb") as f:
                f.write(r.content)
            return True
        else:
            print(f"PDF not found or wrong content type: {pdf_url}")
    except Exception as e:
        print(f"Failed to download {pdf_url}: {e}")
    return False

# メイン処理
success_count = 0
for i, row in tqdm(df.iterrows(), total=len(df)):
    doi = str(row.get("doi", "")).strip()
    if not doi or doi.lower() == "nan":
        continue

    pdf_url = get_pdf_url_unpaywall(doi)
    if pdf_url:
        safe_filename = doi.replace("/", "_").replace(":", "_")
        if download_pdf(pdf_url, f"{safe_filename}.pdf"):
            success_count += 1
    else:
        print(f"PDF URL not found for DOI: {doi}")

print(f"\n Downloaded {success_count} PDFs.")
