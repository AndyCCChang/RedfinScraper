from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse
import re

import requests


PHOTO_STORE_DIR = Path("property_photos")
PHOTO_CACHE_DIR = PHOTO_STORE_DIR / "_cache"
LISTING_PHOTOS_DIR = PHOTO_STORE_DIR / "listings"


def photo_request_headers(referer: str = None) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": referer or "https://www.redfin.com",
    }


def decode_escaped_url(raw: str) -> str:
    return raw.replace("\\u002F", "/").replace("\\/", "/")


def extract_photo_urls_from_html(html: str) -> list:
    matches = re.findall(r'"fullScreenPhotoUrl":"(https:\\u002F\\u002F[^"]+)"', html)
    urls = [decode_escaped_url(match) for match in matches]

    if not urls:
        image_matches = re.findall(r'<img[^>]+src="(https://ssl\.cdn-redfin\.com/[^"]+)"', html)
        urls = image_matches

    unique_urls = []
    seen = set()
    for url in urls:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path
        is_redfin_photo = (
            host == "ssl.cdn-redfin.com"
            and (
                "/photo/" in path
                or "/system_files/media/" in path
            )
        )
        if not is_redfin_photo:
            continue
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    return unique_urls


def fetch_listing_photo_urls(listing_url: str, session: requests.Session = None, timeout: int = 20) -> list:
    client = session or requests.Session()
    response = client.get(
        listing_url,
        headers=photo_request_headers(referer=listing_url),
        timeout=timeout,
    )
    response.raise_for_status()
    return extract_photo_urls_from_html(response.text)


def listing_key_from_url(listing_url: str) -> str:
    parsed = urlparse(listing_url)
    path = parsed.path.strip("/")
    safe = path.replace("/", "__")
    return safe or sha256(listing_url.encode("utf-8")).hexdigest()[:16]


def cache_path_for_photo_url(photo_url: str) -> Path:
    suffix = Path(urlparse(photo_url).path).suffix or ".jpg"
    digest = sha256(photo_url.encode("utf-8")).hexdigest()
    return PHOTO_CACHE_DIR / f"{digest}{suffix}"
