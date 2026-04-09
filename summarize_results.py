from pathlib import Path

import pandas as pd
from pipeline_context import output_path, resolve_input_path


def build_top_deals(df: pd.DataFrame) -> pd.DataFrame:
    deals = df.copy()

    if "days_on_market" in deals.columns:
        deals = deals[deals["days_on_market"].fillna(9999) <= 120]
    if "price" in deals.columns:
        deals = deals[deals["price"].fillna(0) > 0]
    if "sqft" in deals.columns:
        deals = deals[deals["sqft"].fillna(0) > 0]

    # A simple practical ranking:
    # lower price/sqft is better, and newer listings get a small boost.
    deals["deal_score"] = (
        deals["price_per_sqft"].fillna(deals["price_per_sqft"].median()) * 1.0
        + deals["days_on_market"].fillna(60) * 0.35
    ).round(2)

    columns = [
        "full_address",
        "photo_url",
        "zip",
        "price",
        "sqft",
        "price_per_sqft",
        "beds",
        "baths",
        "days_on_market",
        "mls_status",
        "has_virtual_tour",
        "has_3d_tour",
        "deal_score",
        "url",
    ]
    columns = [col for col in columns if col in deals.columns]
    deals = deals[columns].sort_values(
        ["deal_score", "price_per_sqft", "price"],
        ascending=[True, True, True],
        na_position="last",
    )
    return deals.head(20)


def build_zip_compare(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("zip", dropna=False)
        .agg(
            listings=("zip", "size"),
            median_price=("price", "median"),
            avg_price=("price", "mean"),
            median_sqft=("sqft", "median"),
            avg_price_per_sqft=("price_per_sqft", "mean"),
            median_price_per_sqft=("price_per_sqft", "median"),
            median_days_on_market=("days_on_market", "median"),
            avg_beds=("beds", "mean"),
            avg_baths=("baths", "mean"),
        )
        .reset_index()
    )

    numeric_columns = [
        "median_price",
        "avg_price",
        "median_sqft",
        "avg_price_per_sqft",
        "median_price_per_sqft",
        "median_days_on_market",
        "avg_beds",
        "avg_baths",
    ]
    for col in numeric_columns:
        if col in grouped.columns:
            grouped[col] = grouped[col].round(2)

    grouped = grouped.sort_values(
        ["avg_price_per_sqft", "median_price"],
        ascending=[True, True],
        na_position="last",
    )
    return grouped


def main() -> int:
    input_path = resolve_input_path("analysis_ready", ".csv")
    top_deals_path = output_path("top_deals", ".csv", create=True)
    zip_compare_path = output_path("compare_by_zip", ".csv", create=True)

    if not input_path.exists():
        print(f"Missing input file: {input_path.resolve()}")
        return 1

    df = pd.read_csv(input_path)

    top_deals = build_top_deals(df)
    zip_compare = build_zip_compare(df)

    top_deals.to_csv(top_deals_path, index=False)
    zip_compare.to_csv(zip_compare_path, index=False)

    print(f"Saved {len(top_deals)} rows to {top_deals_path.resolve()}")
    print(f"Saved {len(zip_compare)} rows to {zip_compare_path.resolve()}")
    print("\nTop deals preview:")
    print(top_deals.head(10).to_string(index=False))
    print("\nZip comparison preview:")
    print(zip_compare.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
