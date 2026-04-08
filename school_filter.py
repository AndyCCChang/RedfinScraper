from pathlib import Path
import re
import sys

import pandas as pd
from pipeline_context import output_path, resolve_input_path


KEYWORDS = {
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


def score_text(text: str) -> int:
    if not isinstance(text, str):
        return 0

    lowered = text.lower()
    score = 0
    for keyword, weight in KEYWORDS.items():
        if keyword in lowered:
            score += weight
    return score


def extract_matches(text: str) -> str:
    if not isinstance(text, str):
        return ""

    lowered = text.lower()
    matched = [keyword for keyword in KEYWORDS if keyword in lowered]
    return ", ".join(sorted(set(matched)))


def extract_school_name_matches(text: str, school_names) -> str:
    if not isinstance(text, str):
        return ""

    lowered = text.lower()
    matched = [name for name in school_names if name.lower() in lowered]
    return ", ".join(matched)


def main() -> int:
    analysis_path = resolve_input_path("analysis_ready", ".csv")
    results_path = resolve_input_path("results", ".csv")
    school_output_path = output_path("school_homes", ".csv", create=True)
    exact_output_path = output_path("exact_school_homes", ".csv", create=True)

    if not analysis_path.exists() or not results_path.exists():
        print("Missing analysis_ready.csv or results.csv")
        print("Run `python3 run.py` and `python3 clean_results.py` first.")
        return 1

    analysis = pd.read_csv(analysis_path)
    results = pd.read_csv(results_path)

    needed_columns = ["url", "listingRemarks"]
    result_subset = results[[col for col in needed_columns if col in results.columns]].copy()

    merged = analysis.merge(result_subset, on="url", how="left")
    merged["listingRemarks"] = merged["listingRemarks"].fillna("")
    merged["school_score"] = merged["listingRemarks"].apply(score_text)
    merged["school_keywords"] = merged["listingRemarks"].apply(extract_matches)
    school_names = [arg.strip() for arg in sys.argv[1:] if arg.strip()]

    if school_names:
        merged["matched_schools"] = merged["listingRemarks"].apply(
            lambda text: extract_school_name_matches(text, school_names)
        )
        school_homes = merged[merged["matched_schools"] != ""].copy()
        selected_output_path = exact_output_path
    else:
        school_homes = merged[merged["school_score"] > 0].copy()
        selected_output_path = school_output_path

    if school_homes.empty:
        school_homes.to_csv(selected_output_path, index=False)
        print(f"No matching school listings found. Wrote empty file to {selected_output_path.resolve()}")
        return 0

    school_homes["remarks_preview"] = school_homes["listingRemarks"].apply(
        lambda text: re.sub(r"\s+", " ", text).strip()[:220]
    )

    columns = [
        "full_address",
        "zip",
        "price",
        "sqft",
        "price_per_sqft",
        "beds",
        "baths",
        "days_on_market",
        "school_score",
        "school_keywords",
        "matched_schools",
        "remarks_preview",
        "url",
    ]
    columns = [col for col in columns if col in school_homes.columns]
    sort_columns = [col for col in ["school_score", "price_per_sqft", "price"] if col in school_homes.columns]
    ascending = [False, True, True][:len(sort_columns)]
    school_homes = school_homes[columns].sort_values(sort_columns, ascending=ascending, na_position="last")

    school_homes.to_csv(selected_output_path, index=False)

    print(f"Saved {len(school_homes)} school-focused listings to {selected_output_path.resolve()}")
    print(school_homes.head(15).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
