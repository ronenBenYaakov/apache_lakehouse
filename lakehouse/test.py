import os
import re
import json
import csv
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

QUERY = "Ulvik Norway"
OUT_DIR = "pages"

os.makedirs(OUT_DIR, exist_ok=True)

API_KEY = os.getenv("SERPER_API_KEY")
if not API_KEY:
    raise RuntimeError("SERPER_API_KEY not set")

SERPER_URL = "https://google.serper.dev/search"

HEADERS = {
    "X-API-KEY": API_KEY,
    "Content-Type": "application/json"
}

payload = {
    "q": QUERY,
    "gl": "us",
    "hl": "en",
    "num": 10
}

resp = requests.post(SERPER_URL, headers=HEADERS, json=payload, timeout=10)
resp.raise_for_status()

data = resp.json()

def safe_filename(url):
    parsed = urlparse(url)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", parsed.netloc + parsed.path)
    return name.strip("_")[:120]

for idx, item in enumerate(data.get("organic", []), start=1):
    url = item.get("link")
    if not url:
        continue

    try:
        page = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        page.raise_for_status()
    except Exception as e:
        print(f"[SKIP] {url} ({e})")
        continue

    soup = BeautifulSoup(page.text, "html.parser")

    rows = []
    for tag in soup.find_all(["h1", "h2", "h3", "p"]):
        text = tag.get_text(strip=True)
        if text:
            rows.append({
                "query": QUERY,
                "rank": idx,
                "source_url": url,
                "tag": tag.name,
                "text": text
            })

    if not rows:
        print(f"[EMPTY] {url}")
        continue

    fname = safe_filename(url) + ".csv"
    path = os.path.join(OUT_DIR, fname)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["query", "rank", "source_url", "tag", "text"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] {path} ({len(rows)} rows)")
