import concurrent.futures
import json
import os

import pandas as pd
import requests
from photo_utils import fetch_listing_photo_urls
from pipeline_context import (
    output_path,
    resolve_input_path,
    update_latest_analysis_ready_pointer_from_path,
)


PROPERTY_TYPE_MAP = {
    3: "condo",
    6: "house",
    8: "multi_family",
    10: "condo",
    13: "townhouse",
}


SCHOOL_KEYWORDS = {
    "top-rated": 4,
    "top rated": 4,
    "excellent school": 4,
    "excellent schools": 4,
    "good school": 3,
    "good schools": 3,
    "school district": 3,
    "elementary school": 2,
    "middle school": 2,
    "high school": 2,
    "schools": 1,
    "school": 1,
}


ELEMENTARY_SCHOOL_KEYWORDS = {
    "top-rated elementary school": 6,
    "top rated elementary school": 6,
    "excellent elementary school": 5,
    "good elementary school": 4,
    "elementary school": 3,
    "elementary": 1,
}


HIGH_SCHOOL_KEYWORDS = {
    "top-rated high school": 6,
    "top rated high school": 6,
    "excellent high school": 5,
    "good high school": 4,
    "high school": 3,
}


OUTPUT_COLUMNS = {
    "streetLine.value": "address",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "location.value": "neighborhood",
    "price.value": "price",
    "sqFt.value": "sqft",
    "lotSize.value": "lot_size",
    "pricePerSqFt.value": "price_per_sqft_redfin",
    "beds": "beds",
    "baths": "baths",
    "yearBuilt.value": "year_built",
    "dom.value": "days_on_market",
    "mlsStatus": "mls_status",
    "propertyType": "property_type",
    "listingType": "listing_type",
    "skGarageSpaces": "garage_spaces",
    "skParkingSpaces": "parking_spaces",
    "isHot": "is_hot",
    "hasVirtualTour": "has_virtual_tour",
    "has3DTour": "has_3d_tour",
    "listingAgent.name": "listing_agent",
    "listingBroker.name": "listing_broker",
    "latLong.value.latitude": "latitude",
    "latLong.value.longitude": "longitude",
    "url": "url",
}

REDFIN_SCHOOLS_API = "https://www.redfin.com/stingray/api/v1/home/details/belowTheFold/schoolsAndDistrictsInfo"
PROGRESS_EVERY = 10
CLEAN_WORKERS = int(os.environ.get("REDFIN_CLEAN_WORKERS", "12"))


def score_school_text(text: str) -> int:
    if not isinstance(text, str):
        return 0

    lowered = text.lower()
    score = 0
    for keyword, weight in SCHOOL_KEYWORDS.items():
        if keyword in lowered:
            score += weight
    return score


def score_keyword_map(text: str, keywords) -> int:
    if not isinstance(text, str):
        return 0

    lowered = text.lower()
    score = 0
    for keyword, weight in keywords.items():
        if keyword in lowered:
            score += weight
    return score


def school_request_headers(referer: str = None) -> dict:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": referer or "https://www.redfin.com",
    }
    return headers


def decode_redfin_json(response_text: str) -> dict:
    if response_text.startswith("{}&&"):
        response_text = response_text[4:]
    return json.loads(response_text)


def parse_grade_token(token: str):
    cleaned = token.strip().upper()
    if cleaned in {"PK", "TK", "K"}:
        return 0
    if cleaned.isdigit():
        return int(cleaned)
    return None


def grade_span(grade_ranges: str):
    if not isinstance(grade_ranges, str) or not grade_ranges.strip():
        return (None, None)

    normalized = grade_ranges.strip().upper()
    if "-" in normalized:
        left, right = normalized.split("-", 1)
        return parse_grade_token(left), parse_grade_token(right)

    value = parse_grade_token(normalized)
    return value, value


def school_levels(grade_ranges: str):
    min_grade, max_grade = grade_span(grade_ranges)
    levels = set()

    if min_grade is None or max_grade is None:
        return levels

    if min_grade <= 5 and max_grade >= 0:
        levels.add("elementary")
    if min_grade <= 8 and max_grade >= 6:
        levels.add("middle")
    if min_grade <= 12 and max_grade >= 9:
        levels.add("high")

    return levels


def fetch_redfin_school_ratings(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"propertyId", "listingId", "url"}
    if not required_columns.issubset(df.columns):
        print(
            "[clean] Skipping Redfin school ratings; missing columns: %s"
            % ", ".join(sorted(required_columns - set(df.columns))),
            flush=True,
        )
        return pd.DataFrame(index=df.index)

    def empty_school_record() -> dict:
        return {
            "elementary_school_name": pd.NA,
            "elementary_school_rating": pd.NA,
            "middle_school_name": pd.NA,
            "middle_school_rating": pd.NA,
            "high_school_name": pd.NA,
            "high_school_rating": pd.NA,
        }

    def fetch_one(index, row_dict):
        property_id = row_dict.get("propertyId")
        listing_id = row_dict.get("listingId")
        referer = row_dict.get("url")
        school_record = empty_school_record()

        if pd.isna(property_id) or pd.isna(listing_id):
            return index, school_record, "skipped", None

        try:
            params = {
                "propertyId": int(property_id),
                "listingId": int(listing_id),
                "accessLevel": 1,
            }
            with requests.Session() as session:
                response = session.get(
                    REDFIN_SCHOOLS_API,
                    params=params,
                    headers=school_request_headers(referer=referer),
                    timeout=20,
                )
            response.raise_for_status()
            payload = decode_redfin_json(response.text)
            schools = payload.get("payload", {}).get("servingThisHomeSchools", [])
        except Exception as exc:
            return index, school_record, "failed", f"{referer}: {exc}"

        best_by_level = {}
        for school in schools:
            rating = school.get("greatSchoolsRating")
            name = school.get("name")
            levels = school_levels(school.get("gradeRanges"))
            distance = school.get("distanceInMiles")

            try:
                rating_value = float(rating)
            except (TypeError, ValueError):
                continue

            try:
                distance_value = float(distance)
            except (TypeError, ValueError):
                distance_value = float("inf")

            for level in levels:
                current = best_by_level.get(level)
                candidate = (rating_value, -distance_value, name)
                if current is None or candidate > current:
                    best_by_level[level] = candidate

        for level in ("elementary", "middle", "high"):
            candidate = best_by_level.get(level)
            if candidate is None:
                continue
            school_record["%s_school_rating" % level] = candidate[0]
            school_record["%s_school_name" % level] = candidate[2]

        return index, school_record, "ok", None

    records_by_index = {}
    failures = 0
    skipped = 0
    total = len(df)
    completed = 0

    print(f"[clean] Fetching Redfin school ratings for {total} listings with {CLEAN_WORKERS} workers...", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=CLEAN_WORKERS) as executor:
        futures = [
            executor.submit(fetch_one, index, row.to_dict())
            for index, row in df.iterrows()
        ]
        for future in concurrent.futures.as_completed(futures):
            index, school_record, status, error = future.result()
            records_by_index[index] = school_record
            completed += 1
            if status == "skipped":
                skipped += 1
            elif status == "failed":
                failures += 1
                if failures <= 5:
                    print(f"[clean] School rating fetch failed for {error}", flush=True)

            if completed == 1 or completed % PROGRESS_EVERY == 0 or completed == total:
                print(f"[clean] School ratings progress: {completed}/{total}", flush=True)

    records = [records_by_index.get(index, empty_school_record()) for index in df.index]
    print(
        f"[clean] School ratings complete. Records: {len(records)}, skipped: {skipped}, failures: {failures}.",
        flush=True,
    )
    return pd.DataFrame(records, index=df.index)


def fetch_redfin_photo_urls(df: pd.DataFrame) -> pd.Series:
    if "url" not in df.columns:
        print("[clean] Skipping cover photo URLs; missing `url` column.", flush=True)
        return pd.Series(index=df.index, dtype="object")

    def fetch_one(index, listing_url):
        if not isinstance(listing_url, str) or not listing_url.startswith("http"):
            return index, pd.NA, "skipped", None

        try:
            with requests.Session() as session:
                urls = fetch_listing_photo_urls(listing_url, session=session, timeout=20)
            return index, urls[0] if urls else pd.NA, "ok" if urls else "empty", None
        except Exception as exc:
            return index, pd.NA, "failed", f"{listing_url}: {exc}"

    photo_urls_by_index = {}
    failures = 0
    skipped = 0
    found = 0
    total = len(df)
    completed = 0

    print(f"[clean] Fetching cover photo URLs for {total} listings with {CLEAN_WORKERS} workers...", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=CLEAN_WORKERS) as executor:
        futures = [
            executor.submit(fetch_one, index, row.get("url"))
            for index, row in df.iterrows()
        ]
        for future in concurrent.futures.as_completed(futures):
            index, photo_url, status, error = future.result()
            photo_urls_by_index[index] = photo_url
            completed += 1
            if status == "ok":
                found += 1
            elif status == "skipped":
                skipped += 1
            elif status == "failed":
                failures += 1
                if failures <= 5:
                    print(f"[clean] Cover photo fetch failed for {error}", flush=True)

            if completed == 1 or completed % PROGRESS_EVERY == 0 or completed == total:
                print(f"[clean] Cover photo progress: {completed}/{total}", flush=True)

    print(
        f"[clean] Cover photo fetch complete. Found: {found}, skipped: {skipped}, failures: {failures}.",
        flush=True,
    )
    photo_urls = [photo_urls_by_index.get(index, pd.NA) for index in df.index]
    return pd.Series(photo_urls, index=df.index, dtype="object")


def main() -> int:
    input_path = resolve_input_path("results", ".csv")
    output_csv_path = output_path("analysis_ready", ".csv", create=True)

    if not input_path.exists():
        print(f"Missing input file: {input_path.resolve()}")
        return 1

    print(f"[clean] Loading raw results from {input_path.resolve()}", flush=True)
    df = pd.read_csv(input_path)
    print(f"[clean] Loaded {len(df)} raw rows and {len(df.columns)} columns.", flush=True)

    available_columns = [col for col in OUTPUT_COLUMNS if col in df.columns]
    print(f"[clean] Keeping {len(available_columns)} core columns.", flush=True)
    clean = df[available_columns].copy()
    clean = clean.rename(columns=OUTPUT_COLUMNS)

    school_ratings = fetch_redfin_school_ratings(df)
    if not school_ratings.empty:
        print(f"[clean] Adding {len(school_ratings.columns)} Redfin school rating columns.", flush=True)
        for column in school_ratings.columns:
            clean[column] = school_ratings[column]

    print("[clean] Adding cover photo URLs.", flush=True)
    clean["photo_url"] = fetch_redfin_photo_urls(df)

    if "listingRemarks" in df.columns:
        print("[clean] Scoring school-related listing remarks.", flush=True)
        listing_remarks = df["listingRemarks"].fillna("")
        clean["school_score"] = listing_remarks.apply(score_school_text)
        clean["elementary_school_score"] = listing_remarks.apply(
            lambda text: score_keyword_map(text, ELEMENTARY_SCHOOL_KEYWORDS)
        )
        clean["high_school_score"] = listing_remarks.apply(
            lambda text: score_keyword_map(text, HIGH_SCHOOL_KEYWORDS)
        )

    if "property_type" in clean.columns:
        print("[clean] Mapping Redfin property type codes.", flush=True)
        clean["property_type_code"] = pd.to_numeric(clean["property_type"], errors="coerce")
        clean["property_type"] = clean["property_type_code"].map(PROPERTY_TYPE_MAP).fillna("unknown")

    print("[clean] Normalizing numeric columns.", flush=True)
    if "price" in clean.columns:
        clean["price"] = pd.to_numeric(clean["price"], errors="coerce")
    if "sqft" in clean.columns:
        clean["sqft"] = pd.to_numeric(clean["sqft"], errors="coerce")
    if "lot_size" in clean.columns:
        clean["lot_size"] = pd.to_numeric(clean["lot_size"], errors="coerce")
    if "days_on_market" in clean.columns:
        clean["days_on_market"] = pd.to_numeric(clean["days_on_market"], errors="coerce")
    if "elementary_school_rating" in clean.columns:
        clean["elementary_school_rating"] = pd.to_numeric(clean["elementary_school_rating"], errors="coerce")
    if "middle_school_rating" in clean.columns:
        clean["middle_school_rating"] = pd.to_numeric(clean["middle_school_rating"], errors="coerce")
    if "high_school_rating" in clean.columns:
        clean["high_school_rating"] = pd.to_numeric(clean["high_school_rating"], errors="coerce")
    if "garage_spaces" in clean.columns:
        clean["garage_spaces"] = pd.to_numeric(clean["garage_spaces"], errors="coerce")
    if "parking_spaces" in clean.columns:
        clean["parking_spaces"] = pd.to_numeric(clean["parking_spaces"], errors="coerce")

    if "price" in clean.columns and "sqft" in clean.columns:
        clean["price_per_sqft"] = (clean["price"] / clean["sqft"]).round(2)
    else:
        clean["price_per_sqft"] = pd.NA

    if "price" in clean.columns:
        clean["price_k"] = (clean["price"] / 1000).round(1)
    else:
        clean["price_k"] = pd.NA

    if "address" in clean.columns and "city" in clean.columns and "state" in clean.columns:
        clean["full_address"] = (
            clean["address"].fillna("")
            + ", "
            + clean["city"].fillna("")
            + ", "
            + clean["state"].fillna("")
        ).str.strip(", ")
    else:
        clean["full_address"] = pd.NA

    preferred_order = [
        "full_address",
        "address",
        "city",
        "state",
        "zip",
        "neighborhood",
        "price",
        "photo_url",
        "price_k",
        "sqft",
        "lot_size",
        "price_per_sqft",
        "price_per_sqft_redfin",
        "beds",
        "baths",
        "year_built",
        "days_on_market",
        "school_score",
        "elementary_school_name",
        "elementary_school_score",
        "elementary_school_rating",
        "middle_school_name",
        "middle_school_rating",
        "high_school_name",
        "high_school_score",
        "high_school_rating",
        "mls_status",
        "property_type",
        "property_type_code",
        "listing_type",
        "garage_spaces",
        "parking_spaces",
        "is_hot",
        "has_virtual_tour",
        "has_3d_tour",
        "listing_agent",
        "listing_broker",
        "latitude",
        "longitude",
        "url",
    ]
    ordered_columns = [col for col in preferred_order if col in clean.columns]
    print(f"[clean] Ordering {len(ordered_columns)} output columns.", flush=True)
    clean = clean[ordered_columns]

    sort_columns = [col for col in ["price_per_sqft", "price", "days_on_market"] if col in clean.columns]
    if sort_columns:
        print(f"[clean] Sorting by: {', '.join(sort_columns)}", flush=True)
        clean = clean.sort_values(sort_columns, ascending=[True] * len(sort_columns), na_position="last")

    print(f"[clean] Writing cleaned output to {output_csv_path.resolve()}", flush=True)
    clean.to_csv(output_csv_path, index=False)
    update_latest_analysis_ready_pointer_from_path(output_csv_path)

    print(f"Saved {len(clean)} rows to {output_csv_path.resolve()}")
    print(clean.head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
