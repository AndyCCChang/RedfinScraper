from pathlib import Path

import pandas as pd
from pipeline_context import output_path, resolve_input_path


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


def score_school_text(text: str) -> int:
    if not isinstance(text, str):
        return 0

    lowered = text.lower()
    score = 0
    for keyword, weight in SCHOOL_KEYWORDS.items():
        if keyword in lowered:
            score += weight
    return score


def main() -> int:
    input_path = resolve_input_path("results", ".csv")
    output_csv_path = output_path("analysis_ready", ".csv", create=True)

    if not input_path.exists():
        print(f"Missing input file: {input_path.resolve()}")
        return 1

    df = pd.read_csv(input_path)

    available_columns = [col for col in OUTPUT_COLUMNS if col in df.columns]
    clean = df[available_columns].copy()
    clean = clean.rename(columns=OUTPUT_COLUMNS)

    if "listingRemarks" in df.columns:
        clean["school_score"] = df["listingRemarks"].fillna("").apply(score_school_text)

    if "property_type" in clean.columns:
        clean["property_type_code"] = pd.to_numeric(clean["property_type"], errors="coerce")
        clean["property_type"] = clean["property_type_code"].map(PROPERTY_TYPE_MAP).fillna("unknown")

    if "price" in clean.columns:
        clean["price"] = pd.to_numeric(clean["price"], errors="coerce")
    if "sqft" in clean.columns:
        clean["sqft"] = pd.to_numeric(clean["sqft"], errors="coerce")
    if "lot_size" in clean.columns:
        clean["lot_size"] = pd.to_numeric(clean["lot_size"], errors="coerce")
    if "days_on_market" in clean.columns:
        clean["days_on_market"] = pd.to_numeric(clean["days_on_market"], errors="coerce")
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
    clean = clean[ordered_columns]

    sort_columns = [col for col in ["price_per_sqft", "price", "days_on_market"] if col in clean.columns]
    clean = clean.sort_values(sort_columns, ascending=[True, True, True], na_position="last")

    clean.to_csv(output_csv_path, index=False)

    print(f"Saved {len(clean)} rows to {output_csv_path.resolve()}")
    print(clean.head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
