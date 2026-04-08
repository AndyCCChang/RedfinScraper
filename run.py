from pathlib import Path

from redfin_scraper import RedfinScraper
from pipeline_context import output_path


def _recent_log_text() -> str:
    log_path = Path("package.log")
    if not log_path.exists():
        return ""
    try:
        return log_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


def main() -> int:
    scraper = RedfinScraper()
    scraper.setup()
    df = scraper.scrape()

    if df is None or getattr(df, "empty", False):
        log_text = _recent_log_text()
        if "403" in log_text or "Request blocked" in log_text:
            print("No results returned because Redfin blocked the request with HTTP 403.")
            print("This repo can only work from an environment Redfin allows.")
        elif "API link" in log_text:
            print("No results returned because the scraper could not find Redfin's export/API link.")
            print("This usually means Redfin changed the page structure or blocked the page.")
        else:
            print("No results returned. Check config.json and package.log for details.")
        return 1

    results_path = output_path("results", ".csv", create=True)
    df.to_csv(results_path, index=False)

    print(df)
    print(f"\nSaved {len(df)} rows to {results_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
