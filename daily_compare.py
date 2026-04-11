from datetime import datetime
from pathlib import Path

import pandas as pd
from pipeline_context import output_path, resolve_input_path


SNAPSHOT_DIR = Path("snapshots")
def load_snapshot(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "url" not in df.columns:
        raise ValueError(f"Missing required column 'url' in {path}")

    df = df.copy()
    df["url"] = df["url"].astype(str)
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df


def latest_snapshot_path() -> Path:
    if not SNAPSHOT_DIR.exists():
        return None

    snapshot_paths = sorted(SNAPSHOT_DIR.glob("analysis_ready_*.csv"))
    if not snapshot_paths:
        return None

    return snapshot_paths[-1]


def save_current_snapshot(current_df: pd.DataFrame) -> Path:
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = SNAPSHOT_DIR / f"analysis_ready_{timestamp}.csv"
    current_df.to_csv(snapshot_path, index=False)
    return snapshot_path


def build_new_listings(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    previous_urls = set(previous_df["url"].dropna())
    return current_df[~current_df["url"].isin(previous_urls)].copy()


def build_removed_listings(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    current_urls = set(current_df["url"].dropna())
    return previous_df[~previous_df["url"].isin(current_urls)].copy()


def select_columns_with_defaults(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    selected = df[[col for col in columns if col in df.columns]].copy()
    for col in columns:
        if col not in selected.columns:
            selected[col] = pd.NA
    return selected[columns]


def build_price_changes(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    current_prices = select_columns_with_defaults(
        current_df,
        ["url", "full_address", "price", "days_on_market"],
    )
    previous_prices = select_columns_with_defaults(
        previous_df,
        ["url", "full_address", "price"],
    )

    merged = current_prices.merge(
        previous_prices,
        on="url",
        how="inner",
        suffixes=("_current", "_previous"),
    )

    changed = merged[merged["price_current"] != merged["price_previous"]].copy()
    if changed.empty:
        return changed

    changed["price_diff"] = changed["price_current"] - changed["price_previous"]
    changed["price_diff_pct"] = (
        (changed["price_diff"] / changed["price_previous"]) * 100
    ).round(2)

    output_columns = [
        "full_address_current",
        "price_previous",
        "price_current",
        "price_diff",
        "price_diff_pct",
        "days_on_market",
        "url",
    ]
    for col in output_columns:
        if col not in changed.columns:
            changed[col] = pd.NA

    changed = changed[output_columns].rename(
        columns={
            "full_address_current": "full_address",
        }
    )

    sort_columns = [col for col in ["price_diff", "price_current"] if col in changed.columns]
    if sort_columns:
        changed = changed.sort_values(
            sort_columns,
            ascending=[True] * len(sort_columns),
            na_position="last",
        )
    return changed


def main() -> int:
    current_path = resolve_input_path("analysis_ready", ".csv")
    new_listings_path = output_path("new_listings", ".csv", create=True)
    removed_listings_path = output_path("removed_listings", ".csv", create=True)
    price_changes_path = output_path("price_changes", ".csv", create=True)

    if not current_path.exists():
        print(f"Missing input file: {current_path.resolve()}")
        print("Run `python3 clean_results.py` first.")
        return 1

    current_df = load_snapshot(current_path)
    previous_path = latest_snapshot_path()

    if previous_path is None:
        snapshot_path = save_current_snapshot(current_df)
        print(f"No previous snapshot found. Saved current snapshot to {snapshot_path.resolve()}")
        print("Run this script again after the next scrape to compare changes.")
        return 0

    previous_df = load_snapshot(previous_path)

    new_listings = build_new_listings(current_df, previous_df)
    removed_listings = build_removed_listings(current_df, previous_df)
    price_changes = build_price_changes(current_df, previous_df)

    if not new_listings.empty:
        sort_columns = [col for col in ["price_per_sqft", "price"] if col in new_listings.columns]
        if sort_columns:
            new_listings = new_listings.sort_values(
                sort_columns,
                ascending=[True] * len(sort_columns),
                na_position="last",
            )
    if not removed_listings.empty:
        sort_columns = [col for col in ["price_per_sqft", "price"] if col in removed_listings.columns]
        if sort_columns:
            removed_listings = removed_listings.sort_values(
                sort_columns,
                ascending=[True] * len(sort_columns),
                na_position="last",
            )

    new_listings.to_csv(new_listings_path, index=False)
    removed_listings.to_csv(removed_listings_path, index=False)
    price_changes.to_csv(price_changes_path, index=False)

    snapshot_path = save_current_snapshot(current_df)

    print(f"Compared current data to {previous_path.resolve()}")
    print(f"Saved snapshot to {snapshot_path.resolve()}")
    print(f"New listings: {len(new_listings)} -> {new_listings_path.resolve()}")
    print(f"Removed listings: {len(removed_listings)} -> {removed_listings_path.resolve()}")
    print(f"Price changes: {len(price_changes)} -> {price_changes_path.resolve()}")

    if not new_listings.empty:
        print("\nNew listings preview:")
        print(new_listings.head(10).to_string(index=False))

    if not price_changes.empty:
        print("\nPrice changes preview:")
        print(price_changes.head(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
