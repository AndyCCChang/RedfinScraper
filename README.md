# Redfin Scraper

## Description
A scalable Python library that leverages Redfin's unofficial Stringray API to quickly scrape real estate data.

## One-Time Setup
### Zip Code Database
> A database of zip codes is required to search for City, State values  

> It is strongly recommended to download this [free](https://www.unitedstateszipcodes.org/zip-code-database/#) version in .csv format
### The Config
> Parameters for the RedfinScraper class can be controlled using an optional `config.json` file  

> [Sample Config](https://github.com/ryansherby/RedfinScraper/blob/main/config.json)



## Getting Started
### Installation
`pip3 install -U redfin-scraper`

### Local Project Workflow
If you are using this repository directly instead of the published package, you can run the included helper scripts from the repo root.

1. Update `config.json` with your target city or zip codes
2. Run the full pipeline:

`python3 all_in_one.py`

3. Or run the pipeline with a budget filter:

`python3 all_in_one.py 800000 1200000`

4. Or run the pipeline with budget plus home filters:

`python3 all_in_one.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000`

5. Or add price-per-square-foot and zip filters:

`python3 all_in_one.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --max-price-per-sqft 900 --include-zips 95132 95148`

6. Or also require newer listings with virtual tours:

`python3 all_in_one.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --max-price-per-sqft 900 --max-days-on-market 30 --has-virtual-tour --include-zips 95132 95148`

7. Or add garage, parking, and property-type filters:

`python3 all_in_one.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5900 --min-garage-spaces 2 --min-parking-spaces 2 --min-school-score 6 --property-types house townhouse --max-price-per-sqft 900 --include-zips 95132 95148`

8. Or require exact school names too:

`python3 all_in_one.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --min-school-score 6 --school-names "Piedmont Hills High School" "Sierramont Middle School" --property-types house townhouse --include-zips 95132 95148`

This will generate:

- a run folder like `runs/run_20260408_163723/`
- `results_<timestamp>.csv`: Raw scraped listing data
- `analysis_ready_<timestamp>.csv`: Cleaner file with the most useful columns
- `school_homes_<timestamp>.csv`: Listings whose descriptions mention strong school signals
- `top_deals_<timestamp>.csv`: A shortlist ranked by price per square foot and recency
- `compare_by_zip_<timestamp>.csv`: Zip-code summary metrics
- `new_listings_<timestamp>.csv`: Listings not seen in the last snapshot
- `removed_listings_<timestamp>.csv`: Listings that disappeared since the last snapshot
- `price_changes_<timestamp>.csv`: Listings with price changes since the last snapshot
- `budget_matches_<timestamp>.csv`: Listings within your budget range when budget args are provided
- `report_<timestamp>.html`: The HTML report for that run

### Individual Helper Scripts
- `python3 run.py`
  Scrape Redfin and save raw results to `results.csv`
- `python3 clean_results.py`
  Convert `results.csv` into `analysis_ready.csv`
- `python3 school_filter.py`
  Create `school_homes.csv` using school-related keywords in listing descriptions
- `python3 school_filter.py "Piedmont Hills High School" "Sierramont Middle School"`
  Create `exact_school_homes.csv` using exact school-name matches in listing descriptions
- `python3 summarize_results.py`
  Create `top_deals.csv` and `compare_by_zip.csv`
- `python3 budget_filter.py 800000 1200000`
  Create `budget_matches.csv` for a price range
- `python3 budget_filter.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000`
  Create `budget_matches.csv` with budget plus beds, baths, and lot-size filters
- `python3 budget_filter.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --max-price-per-sqft 900 --include-zips 95132 95148`
  Add price-per-square-foot and zip filters to narrow results further
- `python3 budget_filter.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --max-price-per-sqft 900 --max-days-on-market 30 --has-virtual-tour --include-zips 95132 95148`
  Further narrow to newer listings that include a virtual tour
- `python3 budget_filter.py 800000 1500000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --min-garage-spaces 2 --min-parking-spaces 2 --min-school-score 6 --property-types house townhouse --max-price-per-sqft 900 --include-zips 95132 95148`
  Also filter by garage spaces, parking spaces, school-score threshold, and property type names like `house`, `townhouse`, `condo`, or raw codes if needed
- `python3 budget_filter.py 800000 1500000 --min-school-score 6 --school-names "Piedmont Hills High School" "Sierramont Middle School"`
  Further narrow results to listings whose descriptions mention exact target school names
- `python3 daily_compare.py`
  Save a dated snapshot and compare against the previous run

### Import Module
`from redfin_scraper import RedfinScraper`  

### Initialize Module
`scraper = RedfinScraper()`

## Using The Scraper
### Required Setup
`scraper.setup(zip_database_path:str, multiprocessing:bool=False)`

> **zip_database_path**: Binary path to the zip_code_database.csv  

> **multiprocessing**: Allow for multiprocessing

### Activating The Scraper
`scraper.scrape(city_states:list[str]=None, zip_codes:list[str], sold:bool=False, sale_period:str=None, lat_tuner:float=1.5, lon_tuner:float=1.5)`
>**city_states**: List of strings representing US cities formatted as "City, State"  

>**zip_codes**: List of strings representing US zip codes  

>**sold**: Select whether to scrape for-sale data (default) or sold data  

>**sale_period**: Must be selected whenever sold is True (1mo, 3mo, 6mo, 1yr, 3yr, 5yr)

>**lat_tuner**: Represents # of standard deviations beyond the local latitude average that a zip code may exist within   

>**lon_tuner**: Represents # of standard deviations beyond the local longitude average that a zip code may exist within  

### Accessing Prior Scrapes
`scraper.get_data(id:str)`
>**id**: IDs are indexed at 1 and increase in the format "D00#"

## Appendix
### Warnings
> Multiprocessing can result in the consumption of all available CPU resources for an extended period of time  

> Unethical use of this library can result in Redfin taking disciplinary action against your IP address  

> Redfin may change page structure or apply request blocking over time, which can affect scraping reliability  

### Recommendations
> Requests for large amounts of data (# of zip codes > 2,000) should be split into separate requests  

> The `package.log` file can be used to investigate unexpected results
