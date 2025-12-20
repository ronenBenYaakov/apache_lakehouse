import io
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from pyserxng import LocalSearXNGClient, SearchConfig
from pyserxng.models import SearchCategory

from config import create_s3, DEFAULT_BUCKET, RAW_FOLDER
from pipeline import run_pipeline

run_pipeline()