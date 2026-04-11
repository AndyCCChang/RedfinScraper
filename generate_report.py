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
    "mls_status": "Status",
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


def read_text_if_exists(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


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
        ("School Names", ", ".join(str(value) for value in filters.get("school_names", [])) if filters.get("school_names") else None),
        ("Max Price / SqFt", money(filters.get("max_price_per_sqft")) if "max_price_per_sqft" in filters else None),
        ("Max Days On Market", filters.get("max_days_on_market")),
        ("Virtual Tour", "Yes" if filters.get("has_virtual_tour") else None),
        ("Property Types", ", ".join(str(value) for value in filters.get("property_types", [])) if filters.get("property_types") else None),
        ("Included ZIPs", ", ".join(str(value) for value in filters.get("include_zips", [])) if filters.get("include_zips") else None),
        ("Excluded ZIPs", ", ".join(str(value) for value in filters.get("exclude_zips", [])) if filters.get("exclude_zips") else None),
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


def render_search_context(
    search_context: dict,
    config_snapshot: dict,
    user_command: str,
    run_command: str,
    budget_filters: dict,
) -> str:
    if not any([search_context, config_snapshot, user_command, run_command]):
        return ""

    context_rows = [
        ("Run Timestamp", search_context.get("timestamp")),
        ("Pipeline Step", run_command or None),
    ]
    command_filter_rows = [
        ("Budget", f"{money(budget_filters.get('min_price'))} to {money(budget_filters.get('max_price'))}")
        if budget_filters.get("min_price") is not None and budget_filters.get("max_price") is not None
        else None,
        ("Min Beds", budget_filters.get("min_beds")),
        ("Min Baths", budget_filters.get("min_baths")),
        ("Min Lot Size", number(budget_filters.get("min_lot_size")) if budget_filters.get("min_lot_size") is not None else None),
        ("Min Garage", budget_filters.get("min_garage_spaces")),
        ("Min Parking", budget_filters.get("min_parking_spaces")),
        ("Property Types", ", ".join(str(value) for value in budget_filters.get("property_types", [])) if budget_filters.get("property_types") else None),
        ("Included ZIPs", ", ".join(str(value) for value in budget_filters.get("include_zips", [])) if budget_filters.get("include_zips") else None),
        ("Excluded ZIPs", ", ".join(str(value) for value in budget_filters.get("exclude_zips", [])) if budget_filters.get("exclude_zips") else None),
        ("Max Price / SqFt", money(budget_filters.get("max_price_per_sqft")) if budget_filters.get("max_price_per_sqft") is not None else None),
        ("Max Days On Market", budget_filters.get("max_days_on_market")),
        ("Min School Score", budget_filters.get("min_school_score")),
        ("Min Elementary Rating", budget_filters.get("min_elementary_school_score")),
        ("Min High Rating", budget_filters.get("min_high_school_score")),
        ("School Names", ", ".join(str(value) for value in budget_filters.get("school_names", [])) if budget_filters.get("school_names") else None),
        ("Virtual Tour", "Yes" if budget_filters.get("has_virtual_tour") else None),
    ]
    config_rows = [
        ("Cities", ", ".join(str(value) for value in config_snapshot.get("city_states", [])) if config_snapshot.get("city_states") else None),
        ("ZIP Codes", ", ".join(str(value) for value in config_snapshot.get("zip_codes", [])) if config_snapshot.get("zip_codes") else None),
        ("Sold Listings", config_snapshot.get("sold")),
        ("Sale Period", config_snapshot.get("sale_period")),
        ("Multiprocessing", config_snapshot.get("multiprocessing")),
        ("Lat Tuner", config_snapshot.get("lat_tuner")),
        ("Lon Tuner", config_snapshot.get("lon_tuner")),
        ("ZIP Database", config_snapshot.get("zip_database_path")),
    ]

    context_items = [
        f'<div class="filter-item"><span class="filter-key">{escape(str(label))}</span><span class="filter-value">{escape(str(value))}</span></div>'
        for label, value in context_rows
        if value not in (None, "", [])
    ]
    command_filter_items = [
        f'<div class="filter-item"><span class="filter-key">{escape(str(label))}</span><span class="filter-value">{escape(str(value))}</span></div>'
        for entry in command_filter_rows
        if entry is not None
        for label, value in [entry]
        if value not in (None, "", [])
    ]
    config_items = [
        f'<div class="filter-item"><span class="filter-key">{escape(str(label))}</span><span class="filter-value">{escape(str(value))}</span></div>'
        for label, value in config_rows
        if value not in (None, "", [])
    ]

    if not context_items and not config_items:
        return ""

    sections = ['<section><h2>Search Context</h2>']
    if context_items:
        sections.append('<h3 class="section-subtitle">Run Info</h3>')
        sections.append('<div class="filter-grid">%s</div>' % "".join(context_items))
    if command_filter_items:
        sections.append('<h3 class="section-subtitle">Command Filters</h3>')
        sections.append('<div class="filter-grid">%s</div>' % "".join(command_filter_items))
    if config_items:
        sections.append('<h3 class="section-subtitle">Config Conditions</h3>')
        sections.append('<div class="filter-grid">%s</div>' % "".join(config_items))
    if user_command:
        sections.append('<div class="command-line"><span class="filter-key">Full Command</span><code>%s</code></div>' % escape(user_command))
    sections.append("</section>")
    return "".join(sections)


def render_run_panel(user_command: str, config_snapshot: dict, budget_filters: dict) -> str:
    default_command = user_command or "python3 all_in_one.py"
    config_text = json.dumps(config_snapshot or {}, indent=2)
    city_states = "\n".join(str(value) for value in config_snapshot.get("city_states", []))
    zip_codes = "\n".join(str(value) for value in config_snapshot.get("zip_codes", []))
    zip_database_path = config_snapshot.get("zip_database_path", "./zip_code_database.csv")
    multiprocessing = config_snapshot.get("multiprocessing", "False")
    sold = config_snapshot.get("sold", "False")
    sale_period = config_snapshot.get("sale_period", "None")
    lat_tuner = config_snapshot.get("lat_tuner", "1.5")
    lon_tuner = config_snapshot.get("lon_tuner", "1.5")
    budget_min = budget_filters.get("min_price", "")
    budget_max = budget_filters.get("max_price", "")
    min_beds = budget_filters.get("min_beds", "")
    min_baths = budget_filters.get("min_baths", "")
    min_lot_size = budget_filters.get("min_lot_size", "")
    property_type_values = {str(value) for value in budget_filters.get("property_types", [])}
    include_zips = " ".join(str(value) for value in budget_filters.get("include_zips", []))
    exclude_zips = " ".join(str(value) for value in budget_filters.get("exclude_zips", []))
    min_elementary = budget_filters.get("min_elementary_school_score", "")
    min_high = budget_filters.get("min_high_school_score", "")
    school_names = "\n".join(str(value) for value in budget_filters.get("school_names", []))
    return f"""
<section>
  <h2>Run From Page</h2>
  <p class="panel-note">Start the local dashboard server first, then you can launch a fresh pipeline run from this page.</p>
  <div class="run-panel">
    <div class="config-form-grid">
      <div class="config-form-card">
        <label class="panel-label" for="cmd-min-price">Min price</label>
        <input id="cmd-min-price" class="text-input" type="number" step="1000" min="0" inputmode="numeric" value="{escape(str(budget_min))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-max-price">Max price</label>
        <input id="cmd-max-price" class="text-input" type="number" step="1000" min="0" inputmode="numeric" value="{escape(str(budget_max))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-min-beds">Min beds</label>
        <input id="cmd-min-beds" class="text-input" type="number" step="1" min="0" inputmode="numeric" value="{escape(str(min_beds))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-min-baths">Min baths</label>
        <input id="cmd-min-baths" class="text-input" type="number" step="0.5" min="0" inputmode="decimal" value="{escape(str(min_baths))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-min-lot-size">Min lot size</label>
        <input id="cmd-min-lot-size" class="text-input" type="number" step="100" min="0" inputmode="numeric" value="{escape(str(min_lot_size))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label">Property types</label>
        <div class="checkbox-grid">
          <label class="checkbox-option"><input type="checkbox" class="property-type-checkbox" value="house"{" checked" if "house" in property_type_values else ""}> <span>House</span></label>
          <label class="checkbox-option"><input type="checkbox" class="property-type-checkbox" value="townhouse"{" checked" if "townhouse" in property_type_values else ""}> <span>Townhouse</span></label>
          <label class="checkbox-option"><input type="checkbox" class="property-type-checkbox" value="condo"{" checked" if "condo" in property_type_values else ""}> <span>Condo</span></label>
          <label class="checkbox-option"><input type="checkbox" class="property-type-checkbox" value="multi_family"{" checked" if "multi_family" in property_type_values else ""}> <span>Multi-family</span></label>
        </div>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-include-zips">Include ZIPs</label>
        <input id="cmd-include-zips" class="text-input" type="text" value="{escape(include_zips)}" placeholder="95132 95054">
        <div class="helper-text">Use spaces between ZIPs.</div>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-exclude-zips">Exclude ZIPs</label>
        <input id="cmd-exclude-zips" class="text-input" type="text" value="{escape(exclude_zips)}" placeholder="95148 94087">
        <div class="helper-text">Exclude specific ZIPs from the run.</div>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-min-elementary-school-score">Min elementary rating</label>
        <input id="cmd-min-elementary-school-score" class="text-input" type="number" step="1" min="0" max="10" inputmode="numeric" value="{escape(str(min_elementary))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="cmd-min-high-school-score">Min high rating</label>
        <input id="cmd-min-high-school-score" class="text-input" type="number" step="1" min="0" max="10" inputmode="numeric" value="{escape(str(min_high))}">
      </div>
      <div class="config-form-card config-form-card-wide">
        <label class="panel-label" for="cmd-school-names">School names</label>
        <textarea id="cmd-school-names" class="command-input compact-input" spellcheck="false" placeholder="Piedmont Hills High School&#10;Sierramont Middle School">{escape(school_names)}</textarea>
        <div class="helper-text">One school per line.</div>
      </div>
    </div>
    <div class="panel-actions">
      <button id="apply-command-form-button" class="secondary-button" type="button">Apply Form To Command</button>
    </div>
    <label class="panel-label" for="pipeline-command">Command</label>
    <textarea id="pipeline-command" class="command-input" spellcheck="false">{escape(default_command)}</textarea>
    <div class="panel-actions">
      <button id="run-pipeline-button" class="action-button action-button-large" type="button">Run Pipeline</button>
      <button id="refresh-status-button" class="secondary-button" type="button">Refresh Status</button>
    </div>
    <div class="config-form-grid">
      <div class="config-form-card config-form-card-wide">
        <label class="panel-label" for="config-city-states">Cities, one per line</label>
        <textarea id="config-city-states" class="command-input compact-input" spellcheck="false">{escape(city_states)}</textarea>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-zip-codes">ZIP codes, one per line</label>
        <textarea id="config-zip-codes" class="command-input compact-input" spellcheck="false">{escape(zip_codes)}</textarea>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-zip-database-path">ZIP database path</label>
        <input id="config-zip-database-path" class="text-input" type="text" value="{escape(str(zip_database_path))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-multiprocessing">Multiprocessing</label>
        <select id="config-multiprocessing" class="text-input">
          <option value="False"{" selected" if str(multiprocessing) == "False" else ""}>False</option>
          <option value="True"{" selected" if str(multiprocessing) == "True" else ""}>True</option>
        </select>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-sold">Sold listings</label>
        <select id="config-sold" class="text-input">
          <option value="False"{" selected" if str(sold) == "False" else ""}>False</option>
          <option value="True"{" selected" if str(sold) == "True" else ""}>True</option>
        </select>
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-sale-period">Sale period</label>
        <input id="config-sale-period" class="text-input" type="text" value="{escape(str(sale_period))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-lat-tuner">Lat tuner</label>
        <input id="config-lat-tuner" class="text-input" type="number" step="0.1" min="0" inputmode="decimal" value="{escape(str(lat_tuner))}">
      </div>
      <div class="config-form-card">
        <label class="panel-label" for="config-lon-tuner">Lon tuner</label>
        <input id="config-lon-tuner" class="text-input" type="number" step="0.1" min="0" inputmode="decimal" value="{escape(str(lon_tuner))}">
      </div>
    </div>
    <label class="panel-label" for="config-json">Config JSON</label>
    <textarea id="config-json" class="command-input config-input" spellcheck="false">{escape(config_text)}</textarea>
    <div class="panel-actions">
      <button id="apply-form-button" class="secondary-button" type="button">Apply Form To JSON</button>
      <button id="save-config-button" class="secondary-button" type="button">Save Config</button>
      <button id="reload-config-button" class="secondary-button" type="button">Reload Config</button>
    </div>
    <div id="run-status" class="run-status">Waiting for local dashboard server at <code>http://127.0.0.1:8765</code>.</div>
  </div>
</section>
"""


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
    search_context: dict,
    config_snapshot: dict,
    user_command: str,
    run_command: str,
) -> str:
    run_panel_section = render_run_panel(user_command, config_snapshot, budget_filters)
    report_analysis = budget if not budget.empty else analysis
    report_top_deals = budget if not budget.empty else top_deals
    if not budget.empty and not school_homes.empty and "url" in school_homes.columns and "url" in budget.columns:
        filtered_urls = set(budget["url"].dropna().astype(str))
        report_school_homes = school_homes[school_homes["url"].astype(str).isin(filtered_urls)].copy()
    else:
        report_school_homes = school_homes
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
                "mls_status",
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
    if not report_school_homes.empty:
        school_section = render_table(
            report_school_homes.head(15),
            "School-Focused Homes",
            [
                "full_address",
                "photo_url",
                "zip",
                "mls_status",
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
    .section-subtitle {{
      margin: 18px 0 10px;
      font-size: 1rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .command-line {{
      margin-top: 14px;
      padding: 12px 14px;
      background: #fcf7ef;
      border: 1px solid #eadfcf;
      border-radius: 14px;
    }}
    .command-line code {{
      display: block;
      margin-top: 6px;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 0.92rem;
      color: var(--ink);
    }}
    .run-panel {{
      display: grid;
      gap: 12px;
    }}
    .panel-note {{
      margin: 0 0 12px;
      color: var(--muted);
    }}
    .panel-label {{
      font-size: 0.88rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: bold;
    }}
    .command-input {{
      width: 100%;
      min-height: 88px;
      padding: 12px 14px;
      border: 1px solid #d8ccb8;
      border-radius: 14px;
      background: #fffaf2;
      font: inherit;
      line-height: 1.45;
      resize: vertical;
    }}
    .config-input {{
      min-height: 240px;
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      font-size: 0.9rem;
    }}
    .compact-input {{
      min-height: 120px;
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      font-size: 0.92rem;
    }}
    .config-form-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    .config-form-card {{
      background: #fcf7ef;
      border: 1px solid #eadfcf;
      border-radius: 14px;
      padding: 12px 14px;
      display: grid;
      gap: 8px;
    }}
    .checkbox-grid {{
      display: grid;
      gap: 8px;
    }}
    .checkbox-option {{
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.95rem;
      color: var(--ink);
    }}
    .checkbox-option input {{
      width: 16px;
      height: 16px;
    }}
    .config-form-card-wide {{
      grid-column: span 2;
    }}
    .text-input {{
      width: 100%;
      padding: 10px 12px;
      border: 1px solid #d8ccb8;
      border-radius: 12px;
      background: #fffaf2;
      font: inherit;
      color: var(--ink);
    }}
    .text-input::placeholder,
    .command-input::placeholder {{
      color: #8a907f;
    }}
    .helper-text {{
      color: var(--muted);
      font-size: 0.83rem;
      line-height: 1.4;
    }}
    .panel-actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .action-button-large {{
      border: 0;
      cursor: pointer;
      font: inherit;
      min-width: 140px;
      padding: 11px 16px;
    }}
    .secondary-button {{
      border: 1px solid #cdbfa9;
      background: #fffaf2;
      color: var(--ink);
      border-radius: 999px;
      padding: 11px 16px;
      font: inherit;
      cursor: pointer;
    }}
    .run-status {{
      padding: 12px 14px;
      background: #fcf7ef;
      border: 1px solid #eadfcf;
      border-radius: 14px;
      color: var(--ink);
      line-height: 1.45;
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
      {render_summary_cards(report_analysis)}
    </div>

    {run_panel_section}
    {render_table(report_top_deals.head(15), "Top Deals", ["full_address", "photo_url", "zip", "mls_status", "price", "sqft", "price_per_sqft", "beds", "baths", "days_on_market", "deal_score", "url"], link_column="url")}
    {render_table(report_analysis.head(15), "Latest Listings Snapshot", ["full_address", "photo_url", "zip", "property_type", "mls_status", "price", "sqft", "lot_size", "beds", "baths", "elementary_school_name", "elementary_school_rating", "middle_school_name", "middle_school_rating", "high_school_name", "high_school_rating", "garage_spaces", "parking_spaces", "days_on_market", "url"], link_column="url")}
    {school_section}
    {render_table(zip_compare, "Zip Comparison", ["zip", "listings", "median_price", "avg_price", "median_sqft", "avg_price_per_sqft", "median_price_per_sqft", "median_days_on_market", "avg_beds", "avg_baths"])}
    {price_changes_section}
    {budget_section}

    <div class="footer">Generated from local CSV outputs in this repository.</div>
  </div>
  <script>
    const serverBase = "http://127.0.0.1:8765";
    const statusEl = document.getElementById("run-status");
    const commandEl = document.getElementById("pipeline-command");
    const configEl = document.getElementById("config-json");
    const cmdMinPriceEl = document.getElementById("cmd-min-price");
    const cmdMaxPriceEl = document.getElementById("cmd-max-price");
    const cmdMinBedsEl = document.getElementById("cmd-min-beds");
    const cmdMinBathsEl = document.getElementById("cmd-min-baths");
    const cmdMinLotSizeEl = document.getElementById("cmd-min-lot-size");
    const cmdIncludeZipsEl = document.getElementById("cmd-include-zips");
    const cmdExcludeZipsEl = document.getElementById("cmd-exclude-zips");
    const cmdMinElementaryEl = document.getElementById("cmd-min-elementary-school-score");
    const cmdMinHighEl = document.getElementById("cmd-min-high-school-score");
    const cmdSchoolNamesEl = document.getElementById("cmd-school-names");
    const propertyTypeCheckboxes = Array.from(document.querySelectorAll(".property-type-checkbox"));
    const cityStatesEl = document.getElementById("config-city-states");
    const zipCodesEl = document.getElementById("config-zip-codes");
    const zipDbPathEl = document.getElementById("config-zip-database-path");
    const multiprocessingEl = document.getElementById("config-multiprocessing");
    const soldEl = document.getElementById("config-sold");
    const salePeriodEl = document.getElementById("config-sale-period");
    const latTunerEl = document.getElementById("config-lat-tuner");
    const lonTunerEl = document.getElementById("config-lon-tuner");
    const runButton = document.getElementById("run-pipeline-button");
    const refreshButton = document.getElementById("refresh-status-button");
    const applyCommandFormButton = document.getElementById("apply-command-form-button");
    const applyFormButton = document.getElementById("apply-form-button");
    const saveConfigButton = document.getElementById("save-config-button");
    const reloadConfigButton = document.getElementById("reload-config-button");

    function setStatus(message) {{
      statusEl.textContent = message;
    }}

    function splitLines(value) {{
      return value
        .split("\\n")
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    }}

    function splitWords(value) {{
      return value
        .split(/\\s+/)
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    }}

    function selectedPropertyTypes() {{
      return propertyTypeCheckboxes
        .filter((checkbox) => checkbox.checked)
        .map((checkbox) => checkbox.value);
    }}

    function formToConfig() {{
      return {{
        zip_database_path: zipDbPathEl.value.trim() || "./zip_code_database.csv",
        city_states: splitLines(cityStatesEl.value),
        zip_codes: splitLines(zipCodesEl.value),
        multiprocessing: multiprocessingEl.value,
        sold: soldEl.value,
        sale_period: salePeriodEl.value.trim() || "None",
        lat_tuner: latTunerEl.value.trim() || "1.5",
        lon_tuner: lonTunerEl.value.trim() || "1.5"
      }};
    }}

    function configToForm(config) {{
      cityStatesEl.value = (config.city_states || []).join("\\n");
      zipCodesEl.value = (config.zip_codes || []).join("\\n");
      zipDbPathEl.value = config.zip_database_path || "./zip_code_database.csv";
      multiprocessingEl.value = String(config.multiprocessing ?? "False");
      soldEl.value = String(config.sold ?? "False");
      salePeriodEl.value = String(config.sale_period ?? "None");
      latTunerEl.value = String(config.lat_tuner ?? "1.5");
      lonTunerEl.value = String(config.lon_tuner ?? "1.5");
    }}

    function syncJsonFromForm() {{
      configEl.value = JSON.stringify(formToConfig(), null, 2);
    }}

    function addFlag(parts, flag, value) {{
      if (value !== null && value !== undefined && String(value).trim() !== "") {{
        parts.push(flag, String(value).trim());
      }}
    }}

    function addMultiValueFlag(parts, flag, values) {{
      if (values.length > 0) {{
        parts.push(flag, ...values);
      }}
    }}

    function commandFormToCommand() {{
      const parts = ["python3", "all_in_one.py"];
      const minPrice = cmdMinPriceEl.value.trim();
      const maxPrice = cmdMaxPriceEl.value.trim();

      if (minPrice && maxPrice) {{
        parts.push(minPrice, maxPrice);
      }}

      addFlag(parts, "--min-beds", cmdMinBedsEl.value);
      addFlag(parts, "--min-baths", cmdMinBathsEl.value);
      addFlag(parts, "--min-lot-size", cmdMinLotSizeEl.value);
      addFlag(parts, "--min-elementary-school-score", cmdMinElementaryEl.value);
      addFlag(parts, "--min-high-school-score", cmdMinHighEl.value);
      addMultiValueFlag(parts, "--property-types", selectedPropertyTypes());
      addMultiValueFlag(parts, "--include-zips", splitWords(cmdIncludeZipsEl.value));
      addMultiValueFlag(parts, "--exclude-zips", splitWords(cmdExcludeZipsEl.value));
      addMultiValueFlag(parts, "--school-names", splitLines(cmdSchoolNamesEl.value));

      return parts.join(" ");
    }}

    async function refreshStatus() {{
      try {{
        const response = await fetch(`${{serverBase}}/api/status`);
        if (!response.ok) {{
          throw new Error(`HTTP ${{response.status}}`);
        }}
        const data = await response.json();
        if (data.running) {{
          setStatus(`Pipeline is running. Last command: ${{data.last_command || "-"}}`);
        }} else if (data.last_return_code === 0) {{
          setStatus("Last pipeline run finished successfully. Refresh this page after the report is regenerated.");
        }} else if (data.last_return_code !== null) {{
          setStatus(`Last pipeline run finished with exit code ${{data.last_return_code}}.`);
        }} else {{
          setStatus("Dashboard server is ready. You can run a new command from this page.");
        }}
      }} catch (error) {{
        setStatus("Dashboard server is not running. Start it with: python3 dashboard_server.py");
      }}
    }}

    async function loadConfig() {{
      try {{
        const response = await fetch(`${{serverBase}}/api/config`);
        if (!response.ok) {{
          throw new Error(`HTTP ${{response.status}}`);
        }}
        const data = await response.json();
        const config = data.config || {{}};
        configToForm(config);
        configEl.value = JSON.stringify(config, null, 2);
      }} catch (error) {{
        setStatus("Could not load config from dashboard server.");
      }}
    }}

    async function saveConfig() {{
      let parsed;
      try {{
        parsed = JSON.parse(configEl.value);
        configToForm(parsed);
      }} catch (error) {{
        setStatus(`Config JSON is invalid: ${{error.message}}`);
        return;
      }}

      try {{
        const response = await fetch(`${{serverBase}}/api/config`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ config: parsed }})
        }});
        const data = await response.json();
        if (!response.ok || !data.ok) {{
          throw new Error(data.error || `HTTP ${{response.status}}`);
        }}
        setStatus("Config saved successfully.");
      }} catch (error) {{
        setStatus(`Could not save config: ${{error.message}}`);
      }}
    }}

    async function runPipeline() {{
      const command = commandEl.value.trim();
      if (!command) {{
        setStatus("Please enter a command first.");
        return;
      }}

      setStatus("Starting pipeline...");
      try {{
        const response = await fetch(`${{serverBase}}/api/run`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ command }})
        }});
        const data = await response.json();
        if (!response.ok || !data.ok) {{
          throw new Error(data.error || `HTTP ${{response.status}}`);
        }}
        setStatus(`Started: ${{data.command}}`);
        setTimeout(refreshStatus, 1500);
      }} catch (error) {{
        setStatus(`Could not start pipeline: ${{error.message}}`);
      }}
    }}

    runButton.addEventListener("click", runPipeline);
    refreshButton.addEventListener("click", refreshStatus);
    applyCommandFormButton.addEventListener("click", () => {{
      commandEl.value = commandFormToCommand();
      setStatus("Command form applied to command.");
    }});
    applyFormButton.addEventListener("click", () => {{
      syncJsonFromForm();
      setStatus("Config form applied to JSON.");
    }});
    saveConfigButton.addEventListener("click", saveConfig);
    reloadConfigButton.addEventListener("click", loadConfig);
    refreshStatus();
    loadConfig();
  </script>
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
    search_context = read_json_if_exists(resolve_input_path("search_context", ".json"))
    config_snapshot = read_json_if_exists(resolve_input_path("config_used", ".json"))
    user_command = read_text_if_exists(resolve_input_path("user_command_used", ".txt"))
    run_command = read_text_if_exists(resolve_input_path("command_used", ".txt"))
    output_html_path = output_path("report", ".html", create=True)

    html = build_html(
        analysis,
        top_deals,
        zip_compare,
        budget,
        school_homes,
        price_changes,
        budget_filters,
        search_context,
        config_snapshot,
        user_command,
        run_command,
    )
    output_html_path.write_text(html, encoding="utf-8")
    update_latest_report_pointer(run_dir, timestamp)

    print(f"Saved report to {output_html_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
