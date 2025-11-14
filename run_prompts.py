from generate_data import generate_and_save_data
from ingest_data import load_data_into_sqlite
from pipeline_utils import DATA_DIR, DB_PATH
from report_data import print_report_rows


def main() -> None:
    datasets = generate_and_save_data()
    print("Generated datasets:")
    for name, rows in datasets.items():
        print(f"- {name}: {len(rows)} records -> {DATA_DIR / f'{name}.csv'}")

    load_data_into_sqlite()
    print(f"\nData loaded into SQLite database at {DB_PATH}\n")

    print("Sample multi-table report (top rows):")
    print_report_rows(limit=20)


if __name__ == "__main__":
    main()

