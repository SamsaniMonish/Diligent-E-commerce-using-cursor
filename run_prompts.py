import argparse
import random

from generate_data import generate_and_save_data
from ingest_data import load_data_into_sqlite
from pipeline_utils import DATA_DIR, DB_PATH, DEFAULT_REPORT_LIMIT, ensure_base_dirs
from report_data import print_report_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-to-end synthetic e-commerce pipeline runner.")
    parser.add_argument("--customers", type=int, default=150, help="Number of customers to generate.")
    parser.add_argument("--products", type=int, default=200, help="Number of products to generate.")
    parser.add_argument("--orders", type=int, default=300, help="Number of orders to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for generation stage.")
    parser.add_argument("--limit", type=int, default=DEFAULT_REPORT_LIMIT, help="Rows to show in report output.")
    parser.add_argument("--format", choices=["txt", "csv"], default="txt", help="File format for saved report.")
    parser.add_argument("--no-save-report", action="store_true", help="Skip writing the joined report to disk.")
    parser.add_argument("--quiet-load", action="store_true", help="Silence per-table ingest row counts.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_base_dirs()
    random.seed(args.seed)

    datasets = generate_and_save_data(
        customer_count=args.customers,
        product_count=args.products,
        order_count=args.orders,
    )
    print("Generated datasets:")
    for name, rows in datasets.items():
        print(f"- {name}: {len(rows)} records -> {DATA_DIR / f'{name}.csv'} (seed={args.seed})")

    load_data_into_sqlite(verbose=not args.quiet_load)
    print(f"\nData loaded into SQLite database at {DB_PATH}\n")

    print("Sample multi-table report (top rows):")
    print_report_rows(limit=args.limit, save_to_file=not args.no_save_report, fmt=args.format)


if __name__ == "__main__":
    main()

