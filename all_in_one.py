import os
import subprocess
import sys
from pipeline_context import RUN_DIR_ENV, RUN_TS_ENV, start_new_run_context


STEPS = [
    ["python3", "run.py"],
    ["python3", "clean_results.py"],
    ["python3", "school_filter.py"],
    ["python3", "summarize_results.py"],
    ["python3", "daily_compare.py"],
]


def main() -> int:
    cli_args = sys.argv[1:]
    steps = list(STEPS)
    run_dir, run_timestamp = start_new_run_context()
    env = os.environ.copy()
    env[RUN_DIR_ENV] = str(run_dir)
    env[RUN_TS_ENV] = run_timestamp

    if len(cli_args) == 0:
        pass
    elif len(cli_args) >= 2:
        budget_command = ["python3", "budget_filter.py", cli_args[0], cli_args[1]]
        if len(cli_args) > 2:
            budget_command.extend(cli_args[2:])
        steps.append(budget_command)
    else:
        print("Usage: python3 all_in_one.py [min_price max_price [budget_filter flags...]]")
        print('Example: python3 all_in_one.py 800000 1200000 --min-beds 3 --min-baths 2 --min-lot-size 5000 --min-garage-spaces 2 --min-parking-spaces 2 --min-school-score 6 --school-names "Piedmont Hills High School" "Sierramont Middle School" --property-types house townhouse --max-price-per-sqft 900 --max-days-on-market 30 --has-virtual-tour --include-zips 95132 95148')
        return 1

    steps.append(["python3", "generate_report.py"])

    print(f"Run folder: {run_dir.resolve()}")

    for command in steps:
        print(f"\n>>> Running: {' '.join(command)}")
        completed = subprocess.run(command, env=env)
        if completed.returncode != 0:
            print(f"Step failed: {' '.join(command)}")
            return completed.returncode

    print("\nPipeline complete.")
    print(f"Run output folder: {run_dir.resolve()}")
    print("Generated files may include:")
    print(f"- results_{run_timestamp}.csv")
    print(f"- analysis_ready_{run_timestamp}.csv")
    print(f"- school_homes_{run_timestamp}.csv")
    print(f"- top_deals_{run_timestamp}.csv")
    print(f"- compare_by_zip_{run_timestamp}.csv")
    print(f"- new_listings_{run_timestamp}.csv")
    print(f"- removed_listings_{run_timestamp}.csv")
    print(f"- price_changes_{run_timestamp}.csv")
    print(f"- report_{run_timestamp}.html")
    print(f"- budget_matches_{run_timestamp}.csv (when budget args are provided)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
