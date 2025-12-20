import io
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from pyserxng import LocalSearXNGClient, SearchConfig
from pyserxng.models import SearchCategory

from config import create_s3, DEFAULT_BUCKET, RAW_FOLDER
from pipeline import run_pipeline

QUERY = "law"
TARGET_RESULTS = 500
RESULTS_PER_PAGE = 20  # approximate page size

s3 = create_s3()
bucket = s3.Bucket(DEFAULT_BUCKET)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

def safe_filename(url: str) -> str:
    parsed = urlparse(url)
    name = "".join(c if c.isalnum() else "_" for c in parsed.netloc + parsed.path)
    return name.strip("_")[:120] + ".csv"

def extract_rows(html: str, url: str, rank: int):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tag in soup.find_all(["h1", "h2", "h3", "p"]):
        text = tag.get_text(strip=True)
        if text:
            rows.append({
                "query": QUERY,
                "rank": rank,
                "source_url": url,
                "tag": tag.name,
                "text": text
            })
    return rows

seen_urls = set()
ingested = 0
rank = 1
page = 1

with LocalSearXNGClient("http://localhost:32768") as client:
    while ingested < TARGET_RESULTS:
        config = SearchConfig(
            categories=[SearchCategory.GENERAL],
            page=page
        )
        results = client.search(QUERY, config)

        # if no results on this page, stop early
        if not results.results:
            break

        # stop if this page has only 1 result
        if len(results.results) <= 1:
            print(f"[HALT] Page {page} has {len(results.results)} result(s), stopping pagination.")
            break

        for r in results.results:
            url = str(r.url)
            if url.startswith("http://localhost") or url in seen_urls:
                continue
            seen_urls.add(url)

            try:
                page_resp = requests.get(url, headers=HEADERS, timeout=15)
                page_resp.raise_for_status()
            except Exception as e:
                print(f"[SKIP] {url} ({e})")
                continue

            rows = extract_rows(page_resp.text, url, rank)
            if not rows:
                continue

            buffer = io.StringIO()
            writer = csv.DictWriter(
                buffer,
                fieldnames=["query", "rank", "source_url", "tag", "text"]
            )
            writer.writeheader()
            writer.writerows(rows)

            key = RAW_FOLDER + safe_filename(url)
            bucket.put_object(Key=key, Body=buffer.getvalue().encode("utf-8"))

            ingested += 1
            rank += 1
            print(f"[INGESTED] {ingested}/{TARGET_RESULTS} s3://{DEFAULT_BUCKET}/{key}")

            if ingested >= TARGET_RESULTS:
                break

        page += 1  # next page

run_pipeline()