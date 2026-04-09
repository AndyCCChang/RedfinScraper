from html import escape
import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
from photo_utils import LISTING_PHOTOS_DIR, listing_key_from_url
from pipeline_context import ensure_run_context, output_path, resolve_input_path, update_latest_report_pointer


COLUMN_LABELS = {
    "full_address": "Address",
    "photo_url": "Photo",
    "zip": "ZIP",
    "property_type": "Property Type",
    "price": "Price",
    "sqft": "SqFt",
    "lot_size": "Lot Size",
    "price_per_sqft": "Price / SqFt",
    "beds": "Beds",
    "baths": "Baths",
    "garage_spaces": "Garage",
    "parking_spaces": "Parking",
    "days_on_market": "Days On Market",
    "deal_score": "Deal Score",
    "school_score": "School Score",
    "elementary_school_name": "Elementary School",
    "elementary_school_rating": "Elementary Rating",
    "middle_school_name": "Middle School",
    "middle_school_rating": "Middle Rating",
    "high_school_name": "High School",
    "high_school_rating": "High Rating",
    "school_keywords": "School Signals",
    "matched_schools": "Matched Schools",
    "price_previous": "Previous Price",
    "price_current": "Current Price",
    "price_diff": "Price Change",
    "price_diff_pct": "Price Change %",
    "listings": "Listings",
    "median_price": "Median Price",
    "avg_price": "Average Price",
    "median_sqft": "Median SqFt",
    "avg_price_per_sqft": "Average Price / SqFt",
    "median_price_per_sqft": "Median Price / SqFt",
    "median_days_on_market": "Median Days On Market",
    "avg_beds": "Average Beds",
    "avg_baths": "Average Baths",
    "url": "Link",
}


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def read_json_if_exists(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def money(value) -> str:
    if pd.isna(value):
        return "-"
    return f"${value:,.0f}"


def number(value, digits: int = 0) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:,.{digits}f}"


def listing_photo_folder_uri(listing_url: str) -> Optional[str]:
    if not isinstance(listing_url, str) or not listing_url.startswith("http"):
        return None

    listing_dir = LISTING_PHOTOS_DIR / listing_key_from_url(listing_url)
    gallery_path = listing_dir / "index.html"
    if not gallery_path.exists():
        return None

    return gallery_path.resolve().as_uri()


def render_summary_cards(analysis: pd.DataFrame) -> str:
    if analysis.empty:
        return "<p>No analysis data found.</p>"

    total = len(analysis)
    median_price = analysis["price"].median() if "price" in analysis.columns else None
    avg_ppsf = analysis["price_per_sqft"].mean() if "price_per_sqft" in analysis.columns else None
    median_dom = analysis["days_on_market"].median() if "days_on_market" in analysis.columns else None

    cards = [
        ("Total Listings", f"{total}"),
        ("Median Price", money(median_price)),
        ("Avg Price / SqFt", money(avg_ppsf)),
        ("Median Days On Market", number(median_dom)),
    ]

    return "".join(
        f'<div class="card"><div class="label">{escape(label)}</div><div class="value">{escape(value)}</div></div>'
        for label, value in cards
    )


def render_filter_summary(filters: dict) -> str:
    if not filters:
        return ""

    rows = [
        ("Budget", f"{money(filters.get('min_price'))} to {money(filters.get('max_price'))}"),
        ("Min Beds", filters.get("min_beds")),
        ("Min Baths", filters.get("min_baths")),
        ("Min Lot Size", number(filters.get("min_lot_size")) if "min_lot_size" in filters else None),
        ("Min Garage", filters.get("min_garage_spaces")),
        ("Min Parking", filters.get("min_parking_spaces")),
        ("Min School Score", filters.get("min_school_score")),
        ("Min Elementary Rating", filters.get("min_elementary_school_score")),
        ("Min High Rating", filters.get("min_high_school_score")),
        ("School Names", ", ".join(filters.get("school_names", [])) if filters.get("school_names") else None),
        ("Max Price / SqFt", money(filters.get("max_price_per_sqft")) if "max_price_per_sqft" in filters else None),
        ("Max Days On Market", filters.get("max_days_on_market")),
        ("Virtual Tour", "Yes" if filters.get("has_virtual_tour") else None),
        ("Property Types", ", ".join(filters.get("property_types", [])) if filters.get("property_types") else None),
        ("Included ZIPs", ", ".join(filters.get("include_zips", [])) if filters.get("include_zips") else None),
        ("Excluded ZIPs", ", ".join(filters.get("exclude_zips", [])) if filters.get("exclude_zips") else None),
    ]

    items = [
        f'<div class="filter-item"><span class="filter-key">{escape(str(label))}</span><span class="filter-value">{escape(str(value))}</span></div>'
        for label, value in rows
        if value is not None
    ]

    if not items:
        return ""

    return (
        "<section><h2>Filter Summary</h2>"
        '<div class="filter-grid">%s</div>'
        "</section>"
    ) % "".join(items)


def render_table(df: pd.DataFrame, title: str, columns: List[str], link_column: Optional[str] = None) -> str:
    if df.empty:
        return f"<section><h2>{escape(title)}</h2><p>No data available.</p></section>"

    safe_columns = [col for col in columns if col in df.columns]
    table_df = df[safe_columns].copy()

    for col in table_df.columns:
        if "price" in col:
            table_df[col] = table_df[col].apply(money)
        elif "sqft" in col or "days" in col or col.startswith("avg_") or col.startswith("median_"):
            table_df[col] = table_df[col].apply(lambda v: number(v, 2) if "avg_" in col or "median_" in col else number(v))

    first_column = safe_columns[0] if safe_columns else None
    header_cells = []
    for col in safe_columns:
        classes = []
        if col == first_column:
            classes.append("sticky-first-col")
        if link_column and col == link_column:
            classes.append("sticky-link-col")
        class_attr = f' class="{" ".join(classes)}"' if classes else ""
        header_cells.append(f"<th{class_attr}>{escape(COLUMN_LABELS.get(col, col))}</th>")
    headers = "".join(header_cells)
    rows = []
    for _, row in table_df.iterrows():
        cells = []
        for col in safe_columns:
            value = row[col]
            classes = []
            if col == first_column:
                classes.append("sticky-first-col")
            if link_column and col == link_column:
                classes.append("sticky-link-col")
            class_attr = f' class="{" ".join(classes)}"' if classes else ""
            if link_column and col == link_column and isinstance(value, str) and value.startswith("http"):
                display = "Open"
                cells.append(
                    f'<td{class_attr}><a href="{escape(value)}" target="_blank" rel="noreferrer" class="action-button">{display}</a></td>'
                )
            elif col == "photo_url" and isinstance(value, str) and value.startswith("http"):
                album_uri = listing_photo_folder_uri(row.get("url"))
                if album_uri:
                    cells.append(
                        f'<td{class_attr}><a href="{escape(album_uri)}" target="_blank" rel="noreferrer" class="photo-link">'
                        f'<img src="{escape(value)}" alt="Listing photo" class="listing-photo">'
                        "</a></td>"
                    )
                else:
                    cells.append(
                        f'<td{class_attr}><img src="{escape(value)}" alt="Listing photo" class="listing-photo"></td>'
                    )
            else:
                cells.append(f"<td{class_attr}>{escape(str(value))}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")

    return (
        f"<section><h2>{escape(title)}</h2>"
        f"<div class=\"table-wrap\"><table><thead><tr>{headers}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div></section>"
    )


def build_html(
    analysis: pd.DataFrame,
    top_deals: pd.DataFrame,
    zip_compare: pd.DataFrame,
    budget: pd.DataFrame,
    school_homes: pd.DataFrame,
    price_changes: pd.DataFrame,
    budget_filters: dict,
) -> str:
    filter_summary_section = render_filter_summary(budget_filters)
    budget_section = ""
    if not budget.empty:
        budget_section = render_table(
            budget.head(15),
            "Budget Matches",
            [
                "full_address",
                "photo_url",
                "zip",
                "property_type",
                "price",
                "sqft",
                "lot_size",
                "price_per_sqft",
                "beds",
                "baths",
                "elementary_school_name",
                "elementary_school_rating",
                "middle_school_name",
                "middle_school_rating",
                "high_school_name",
                "high_school_rating",
                "matched_schools",
                "garage_spaces",
                "parking_spaces",
                "days_on_market",
                "url",
            ],
            link_column="url",
        )

    school_section = ""
    if not school_homes.empty:
        school_section = render_table(
            school_homes.head(15),
            "School-Focused Homes",
            [
                "full_address",
                "photo_url",
                "zip",
                "price",
                "sqft",
                "price_per_sqft",
                "elementary_school_name",
                "elementary_school_rating",
                "middle_school_name",
                "middle_school_rating",
                "high_school_name",
                "high_school_rating",
                "school_score",
                "school_keywords",
                "url",
            ],
            link_column="url",
        )

    price_changes_section = ""
    if not price_changes.empty:
        price_changes_section = render_table(
            price_changes.head(15),
            "Price Changes",
            ["full_address", "price_previous", "price_current", "price_diff", "price_diff_pct", "days_on_market", "url"],
            link_column="url",
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redfin Scraper Report</title>
  <style>
    :root {{
      --bg: #f7f3ea;
      --paper: #fffdf8;
      --ink: #1f2933;
      --muted: #5b6470;
      --accent: #0b6e4f;
      --accent-soft: #d8efe6;
      --border: #dccfb8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top left, #fff7df 0, transparent 28%),
        linear-gradient(180deg, #f7f3ea 0%, #efe6d6 100%);
      color: var(--ink);
    }}
    .page {{
      max-width: 1680px;
      margin: 0 auto;
      padding: 32px 24px 48px;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(11,110,79,0.95), rgba(48,86,211,0.86));
      color: white;
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(17, 24, 39, 0.16);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 2rem;
      line-height: 1.1;
    }}
    .hero p {{
      margin: 0;
      color: rgba(255,255,255,0.88);
      max-width: 780px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin: 22px 0 28px;
    }}
    .card {{
      background: var(--paper);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(80, 63, 32, 0.08);
    }}
    .label {{
      color: var(--muted);
      font-size: 0.85rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 8px;
    }}
    .value {{
      font-size: 1.6rem;
      font-weight: bold;
    }}
    section {{
      margin-top: 28px;
      background: rgba(255, 253, 248, 0.88);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(80, 63, 32, 0.06);
    }}
    h2 {{
      margin: 0 0 14px;
      font-size: 1.35rem;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 1200px;
    }}
    th, td {{
      padding: 10px 10px;
      border-bottom: 1px solid #eadfcf;
      text-align: left;
      vertical-align: top;
      font-size: 0.9rem;
      white-space: nowrap;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #f8f1e5;
    }}
    tr:hover td {{
      background: #fcf7ef;
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
      font-weight: bold;
    }}
    .action-button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 72px;
      color: white;
      background: linear-gradient(135deg, #0b6e4f, #1f8d68);
      padding: 8px 12px;
      border-radius: 999px;
      line-height: 1;
      box-shadow: 0 6px 14px rgba(11, 110, 79, 0.22);
    }}
    .listing-photo {{
      width: 120px;
      height: 80px;
      object-fit: cover;
      border-radius: 10px;
      display: block;
      box-shadow: 0 6px 16px rgba(17, 24, 39, 0.14);
    }}
    .photo-link {{
      display: inline-block;
    }}
    .sticky-link-col {{
      position: sticky;
      right: 0;
      background: #fffaf2;
      z-index: 2;
      box-shadow: -8px 0 12px rgba(80, 63, 32, 0.06);
    }}
    .sticky-first-col {{
      position: sticky;
      left: 0;
      background: #fffaf2;
      z-index: 2;
      box-shadow: 8px 0 12px rgba(80, 63, 32, 0.06);
    }}
    th.sticky-link-col {{
      z-index: 3;
      background: #f8f1e5;
    }}
    th.sticky-first-col {{
      z-index: 3;
      background: #f8f1e5;
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .filter-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    .filter-item {{
      background: #fcf7ef;
      border: 1px solid #eadfcf;
      border-radius: 14px;
      padding: 12px 14px;
    }}
    .filter-key {{
      display: block;
      color: var(--muted);
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 4px;
    }}
    .filter-value {{
      display: block;
      font-size: 1rem;
      font-weight: bold;
    }}
    @media (max-width: 720px) {{
      .hero h1 {{ font-size: 1.65rem; }}
      .page {{ padding: 20px 14px 36px; }}
      section {{ padding: 14px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <h1>Redfin Market Snapshot</h1>
      <p>Use this page to scan the current market, spot the most attractive listings, and compare nearby zip codes without digging through raw CSV files.</p>
    </div>

    <div class="cards">
      {render_summary_cards(analysis)}
    </div>

    {filter_summary_section}
    {render_table(top_deals.head(15), "Top Deals", ["full_address", "photo_url", "zip", "price", "sqft", "price_per_sqft", "beds", "baths", "days_on_market", "deal_score", "url"], link_column="url")}
    {render_table(analysis.head(15), "Latest Listings Snapshot", ["full_address", "photo_url", "zip", "property_type", "price", "sqft", "lot_size", "beds", "baths", "elementary_school_name", "elementary_school_rating", "middle_school_name", "middle_school_rating", "high_school_name", "high_school_rating", "garage_spaces", "parking_spaces", "days_on_market", "url"], link_column="url")}
    {school_section}
    {render_table(zip_compare, "Zip Comparison", ["zip", "listings", "median_price", "avg_price", "median_sqft", "avg_price_per_sqft", "median_price_per_sqft", "median_days_on_market", "avg_beds", "avg_baths"])}
    {price_changes_section}
    {budget_section}

    <div class="footer">Generated from local CSV outputs in this repository.</div>
  </div>
</body>
</html>"""


def main() -> int:
    run_dir, timestamp = ensure_run_context(create=True)
    analysis = read_csv_if_exists(resolve_input_path("analysis_ready", ".csv"))
    top_deals = read_csv_if_exists(resolve_input_path("top_deals", ".csv"))
    zip_compare = read_csv_if_exists(resolve_input_path("compare_by_zip", ".csv"))
    budget = read_csv_if_exists(resolve_input_path("budget_matches", ".csv"))
    budget_filters = read_json_if_exists(resolve_input_path("budget_filters", ".json"))
    school_homes = read_csv_if_exists(resolve_input_path("school_homes", ".csv"))
    price_changes = read_csv_if_exists(resolve_input_path("price_changes", ".csv"))
    output_html_path = output_path("report", ".html", create=True)

    html = build_html(analysis, top_deals, zip_compare, budget, school_homes, price_changes, budget_filters)
    output_html_path.write_text(html, encoding="utf-8")
    update_latest_report_pointer(run_dir, timestamp)

    print(f"Saved report to {output_html_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
