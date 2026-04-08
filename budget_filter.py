from pathlib import Path
import argparse

import pandas as pd
from pipeline_context import output_path, resolve_input_path


def parse_budget_arg(raw: str) -> float:
    cleaned = raw.replace(",", "").replace("$", "").strip()
    return float(cleaned)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter analysis_ready.csv by budget and optional home criteria."
    )
    parser.add_argument("min_price", type=parse_budget_arg)
    parser.add_argument("max_price", type=parse_budget_arg)
    parser.add_argument("--min-beds", type=float, default=None)
    parser.add_argument("--min-baths", type=float, default=None)
    parser.add_argument("--min-lot-size", type=float, default=None)
    parser.add_argument("--min-garage-spaces", type=float, default=None)
    parser.add_argument("--min-parking-spaces", type=float, default=None)
    parser.add_argument("--min-school-score", type=float, default=None)
    parser.add_argument("--school-names", nargs="+", default=None)
    parser.add_argument("--max-price-per-sqft", type=float, default=None)
    parser.add_argument("--max-days-on-market", type=float, default=None)
    parser.add_argument("--has-virtual-tour", action="store_true")
    parser.add_argument("--property-types", nargs="+", default=None)
    parser.add_argument("--include-zips", nargs="+", default=None)
    parser.add_argument("--exclude-zips", nargs="+", default=None)
    return parser


def main() -> int:
    input_path = resolve_input_path("analysis_ready", ".csv")
    results_path = resolve_input_path("results", ".csv")
    output_csv_path = output_path("budget_matches", ".csv", create=True)

    if not input_path.exists():
        print(f"Missing input file: {input_path.resolve()}")
        return 1

    args = build_parser().parse_args()

    min_price = args.min_price
    max_price = args.max_price

    if min_price > max_price:
        print("min_price must be less than or equal to max_price")
        return 1

    df = pd.read_csv(input_path)
    results_df = None
    if args.school_names is not None:
        if not results_path.exists():
            print(f"Missing results file for school-name matching: {results_path.resolve()}")
            return 1
        results_df = pd.read_csv(results_path, usecols=["url", "listingRemarks"])

    filtered = df.copy()
    filtered["price"] = pd.to_numeric(filtered["price"], errors="coerce")
    filtered["sqft"] = pd.to_numeric(filtered["sqft"], errors="coerce")
    if "lot_size" in filtered.columns:
        filtered["lot_size"] = pd.to_numeric(filtered["lot_size"], errors="coerce")
    if "beds" in filtered.columns:
        filtered["beds"] = pd.to_numeric(filtered["beds"], errors="coerce")
    if "baths" in filtered.columns:
        filtered["baths"] = pd.to_numeric(filtered["baths"], errors="coerce")
    if "garage_spaces" in filtered.columns:
        filtered["garage_spaces"] = pd.to_numeric(filtered["garage_spaces"], errors="coerce")
    if "parking_spaces" in filtered.columns:
        filtered["parking_spaces"] = pd.to_numeric(filtered["parking_spaces"], errors="coerce")
    if "school_score" in filtered.columns:
        filtered["school_score"] = pd.to_numeric(filtered["school_score"], errors="coerce")
    filtered["price_per_sqft"] = pd.to_numeric(filtered["price_per_sqft"], errors="coerce")
    filtered["days_on_market"] = pd.to_numeric(filtered["days_on_market"], errors="coerce")
    if "zip" in filtered.columns:
        filtered["zip"] = filtered["zip"].astype(str)
    if "property_type" in filtered.columns:
        filtered["property_type"] = filtered["property_type"].astype(str)
    if "property_type_code" in filtered.columns:
        filtered["property_type_code"] = pd.to_numeric(filtered["property_type_code"], errors="coerce")
    if "has_virtual_tour" in filtered.columns:
        filtered["has_virtual_tour"] = filtered["has_virtual_tour"].fillna(False).astype(bool)

    filtered = filtered[
        filtered["price"].between(min_price, max_price, inclusive="both")
    ].copy()

    if args.min_beds is not None and "beds" in filtered.columns:
        filtered = filtered[filtered["beds"].fillna(0) >= args.min_beds].copy()

    if args.min_baths is not None and "baths" in filtered.columns:
        filtered = filtered[filtered["baths"].fillna(0) >= args.min_baths].copy()

    if args.min_lot_size is not None and "lot_size" in filtered.columns:
        filtered = filtered[filtered["lot_size"].fillna(0) >= args.min_lot_size].copy()

    if args.min_garage_spaces is not None and "garage_spaces" in filtered.columns:
        filtered = filtered[filtered["garage_spaces"].fillna(0) >= args.min_garage_spaces].copy()

    if args.min_parking_spaces is not None and "parking_spaces" in filtered.columns:
        filtered = filtered[filtered["parking_spaces"].fillna(0) >= args.min_parking_spaces].copy()

    if args.min_school_score is not None and "school_score" in filtered.columns:
        filtered = filtered[filtered["school_score"].fillna(0) >= args.min_school_score].copy()

    if args.school_names is not None:
        school_names = [name.strip() for name in args.school_names if name.strip()]
        school_name_lookup = results_df.copy()
        school_name_lookup["listingRemarks"] = school_name_lookup["listingRemarks"].fillna("").astype(str)

        def match_school_names(text: str) -> str:
            lowered = text.lower()
            matched = [name for name in school_names if name.lower() in lowered]
            return ", ".join(matched)

        school_name_lookup["matched_schools"] = school_name_lookup["listingRemarks"].apply(match_school_names)
        school_name_lookup = school_name_lookup[["url", "matched_schools"]]
        filtered = filtered.merge(school_name_lookup, on="url", how="left")
        filtered["matched_schools"] = filtered["matched_schools"].fillna("")
        filtered = filtered[filtered["matched_schools"] != ""].copy()

    if args.max_price_per_sqft is not None:
        filtered = filtered[filtered["price_per_sqft"].fillna(float("inf")) <= args.max_price_per_sqft].copy()

    if args.max_days_on_market is not None:
        filtered = filtered[filtered["days_on_market"].fillna(float("inf")) <= args.max_days_on_market].copy()

    if args.has_virtual_tour and "has_virtual_tour" in filtered.columns:
        filtered = filtered[filtered["has_virtual_tour"] == True].copy()

    if args.property_types is not None and "property_type" in filtered.columns:
        requested_values = {str(value).strip().lower() for value in args.property_types}
        property_type_mask = filtered["property_type"].str.lower().isin(requested_values)
        if "property_type_code" in filtered.columns:
            property_type_mask = property_type_mask | filtered["property_type_code"].astype("Int64").astype(str).isin(requested_values)
        filtered = filtered[property_type_mask].copy()

    if args.include_zips is not None and "zip" in filtered.columns:
        include_zips = {str(zip_code) for zip_code in args.include_zips}
        filtered = filtered[filtered["zip"].isin(include_zips)].copy()

    if args.exclude_zips is not None and "zip" in filtered.columns:
        exclude_zips = {str(zip_code) for zip_code in args.exclude_zips}
        filtered = filtered[~filtered["zip"].isin(exclude_zips)].copy()

    filtered = filtered.sort_values(
        ["price_per_sqft", "price", "days_on_market"],
        ascending=[True, True, True],
        na_position="last",
    )

    filtered.to_csv(output_csv_path, index=False)

    print(
        f"Saved {len(filtered)} rows between ${min_price:,.0f} and ${max_price:,.0f} "
        f"to {output_csv_path.resolve()}"
    )
    if args.min_beds is not None:
        print(f"Minimum beds: {args.min_beds}")
    if args.min_baths is not None:
        print(f"Minimum baths: {args.min_baths}")
    if args.min_lot_size is not None:
        print(f"Minimum lot size: {args.min_lot_size:,.0f}")
    if args.min_garage_spaces is not None:
        print(f"Minimum garage spaces: {args.min_garage_spaces:,.0f}")
    if args.min_parking_spaces is not None:
        print(f"Minimum parking spaces: {args.min_parking_spaces:,.0f}")
    if args.min_school_score is not None:
        print(f"Minimum school score: {args.min_school_score:,.0f}")
    if args.school_names is not None:
        print(f"School names: {', '.join(args.school_names)}")
    if args.max_price_per_sqft is not None:
        print(f"Maximum price per sqft: {args.max_price_per_sqft:,.2f}")
    if args.max_days_on_market is not None:
        print(f"Maximum days on market: {args.max_days_on_market:,.0f}")
    if args.has_virtual_tour:
        print("Virtual tour required: True")
    if args.property_types is not None:
        print(f"Property types: {', '.join(args.property_types)}")
    if args.include_zips is not None:
        print(f"Included zips: {', '.join(args.include_zips)}")
    if args.exclude_zips is not None:
        print(f"Excluded zips: {', '.join(args.exclude_zips)}")
    print(filtered.head(15).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
