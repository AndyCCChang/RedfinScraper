import json
import os
from pathlib import Path

import pandas as pd
import requests

from photo_utils import (
    PHOTO_CACHE_DIR,
    LISTING_PHOTOS_DIR,
    cache_path_for_photo_url,
    fetch_listing_photo_urls,
    listing_key_from_url,
)
from pipeline_context import resolve_input_path


PROGRESS_EVERY = 10


def ensure_dirs() -> None:
    PHOTO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LISTING_PHOTOS_DIR.mkdir(parents=True, exist_ok=True)


def download_to_cache(photo_url: str, session: requests.Session) -> tuple:
    cache_path = cache_path_for_photo_url(photo_url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        return cache_path, False

    response = session.get(photo_url, timeout=30, stream=True)
    response.raise_for_status()

    tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    with tmp_path.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 128):
            if chunk:
                fh.write(chunk)
    os.replace(tmp_path, cache_path)
    return cache_path, True


def safe_link_target(target: Path, link_path: Path) -> Path:
    return Path(os.path.relpath(str(target), start=str(link_path.parent)))


def render_gallery_html(listing_title: str, listing_url: str, photo_files: list) -> str:
    gallery_items = []
    for photo_name in photo_files:
        gallery_items.append(
            f'<a class="photo-card" href="{photo_name}" target="_blank" rel="noreferrer">'
            f'<img src="{photo_name}" alt="{listing_title} photo">'
            f'<span>{photo_name}</span>'
            "</a>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{listing_title} Photos</title>
  <style>
    :root {{
      --bg: #f5efe3;
      --paper: #fffaf2;
      --ink: #1f2933;
      --muted: #5b6470;
      --accent: #0b6e4f;
      --border: #dccfb8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: linear-gradient(180deg, #f7f3ea 0%, #efe6d6 100%);
      color: var(--ink);
    }}
    .page {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }}
    .hero {{
      background: var(--paper);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 20px 22px;
      box-shadow: 0 10px 24px rgba(80, 63, 32, 0.08);
      margin-bottom: 20px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 1.8rem;
    }}
    .hero a {{
      color: var(--accent);
      text-decoration: none;
      font-weight: bold;
    }}
    .gallery {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
    }}
    .photo-card {{
      display: block;
      background: var(--paper);
      border: 1px solid var(--border);
      border-radius: 18px;
      overflow: hidden;
      text-decoration: none;
      color: inherit;
      box-shadow: 0 8px 20px rgba(80, 63, 32, 0.06);
    }}
    .photo-card img {{
      display: block;
      width: 100%;
      height: 190px;
      object-fit: cover;
      background: #eee4d3;
    }}
    .photo-card span {{
      display: block;
      padding: 10px 12px 12px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <h1>{listing_title}</h1>
      <a href="{listing_url}" target="_blank" rel="noreferrer">Open listing on Redfin</a>
    </div>
    <div class="gallery">
      {''.join(gallery_items)}
    </div>
  </div>
</body>
</html>"""


def write_listing_links(listing_dir: Path, photo_urls: list, cache_paths: list, listing_title: str, listing_url: str) -> None:
    listing_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "listing_title": listing_title,
        "listing_url": listing_url,
        "photos": [],
    }

    desired_files = []
    photo_files = []
    for index, (photo_url, cache_path) in enumerate(zip(photo_urls, cache_paths), start=1):
        suffix = cache_path.suffix or ".jpg"
        photo_name = f"{index:03d}{suffix}"
        photo_path = listing_dir / photo_name
        desired_files.append(photo_name)
        photo_files.append(photo_name)

        if photo_path.exists() or photo_path.is_symlink():
            photo_path.unlink()

        try:
            photo_path.symlink_to(safe_link_target(cache_path, photo_path))
        except OSError:
            photo_path.write_bytes(cache_path.read_bytes())

        manifest["photos"].append(
            {
                "index": index,
                "source_url": photo_url,
                "cache_file": str(cache_path),
                "listing_file": str(photo_path),
            }
        )

    for existing in listing_dir.iterdir():
        if existing.name in {"manifest.json", "index.html"}:
            continue
        if existing.name not in desired_files:
            if existing.is_file() or existing.is_symlink():
                existing.unlink()

    (listing_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (listing_dir / "index.html").write_text(
        render_gallery_html(listing_title=listing_title, listing_url=listing_url, photo_files=photo_files),
        encoding="utf-8",
    )


def main() -> int:
    ensure_dirs()

    input_path = resolve_input_path("analysis_ready", ".csv")
    if not input_path.exists():
        print(f"Missing input file: {input_path.resolve()}")
        print("Run `python3 clean_results.py` first.")
        return 1

    df = pd.read_csv(input_path)
    if "url" not in df.columns:
        print("analysis_ready.csv is missing the `url` column.")
        return 1

    print(f"Loading listings from {input_path.resolve()}", flush=True)
    print(f"Preparing to archive photos for {len(df)} cleaned listings.", flush=True)

    session = requests.Session()
    total_listings = 0
    total_downloaded = 0
    total_cached = 0
    failures = 0
    skipped = 0
    processed_with_photos = 0

    for _, row in df.iterrows():
        listing_url = row.get("url")
        if not isinstance(listing_url, str) or not listing_url.startswith("http"):
            skipped += 1
            continue

        total_listings += 1
        listing_dir = LISTING_PHOTOS_DIR / listing_key_from_url(listing_url)
        listing_title = str(row.get("full_address") or listing_dir.name)

        if total_listings == 1 or total_listings % PROGRESS_EVERY == 0:
            print(
                f"[photos] Processing listing {total_listings}: {listing_title}",
                flush=True,
            )

        try:
            photo_urls = fetch_listing_photo_urls(listing_url, session=session, timeout=30)
            if not photo_urls:
                print(f"[photos] No photos found for: {listing_title}", flush=True)
                continue

            cache_paths = []
            for photo_url in photo_urls:
                cache_path, downloaded = download_to_cache(photo_url, session=session)
                cache_paths.append(cache_path)
                if downloaded:
                    total_downloaded += 1
                else:
                    total_cached += 1

            write_listing_links(
                listing_dir,
                photo_urls,
                cache_paths,
                listing_title=listing_title,
                listing_url=listing_url,
            )
            processed_with_photos += 1

            if total_listings == 1 or total_listings % PROGRESS_EVERY == 0:
                print(
                    f"[photos] Saved {len(photo_urls)} photos for {listing_title} "
                    f"(downloaded so far: {total_downloaded}, reused cache: {total_cached})",
                    flush=True,
                )
        except Exception as exc:
            failures += 1
            print(f"[photos] Failed: {listing_title} ({exc})", flush=True)

    print(f"Processed {total_listings} listings.")
    print(f"Listings with photos saved: {processed_with_photos}")
    print(f"Skipped listings without valid URLs: {skipped}")
    print(f"Downloaded {total_downloaded} new photos.")
    print(f"Reused {total_cached} cached photos.")
    print(f"Listing folders saved under {LISTING_PHOTOS_DIR.resolve()}")
    print(f"Shared photo cache stored under {PHOTO_CACHE_DIR.resolve()}")
    if failures:
        print(f"Failed listings: {failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
