import argparse
import csv
import sqlite3
from datetime import datetime
from io import StringIO
from typing import List, Tuple

from pipeline_utils import DB_PATH, DEFAULT_REPORT_LIMIT, REPORTS_DIR, ensure_reports_dir


def run_report(limit: int = 20) -> List[Tuple]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                c.customer_id,
                c.first_name || ' ' || c.last_name AS customer_name,
                c.city,
                c.state,
                o.order_id,
                o.order_date,
                o.status,
                oi.product_id,
                p.name AS product_name,
                oi.quantity,
                oi.unit_price,
                oi.line_total,
                pay.amount AS payment_amount,
                pay.method AS payment_method
            FROM customers c
            JOIN orders o ON o.customer_id = c.customer_id
            JOIN order_items oi ON oi.order_id = o.order_id
            JOIN products p ON p.product_id = oi.product_id
            JOIN payments pay ON pay.order_id = o.order_id
            ORDER BY o.order_date DESC
            LIMIT ?;
            """,
            (limit,),
        )
        return cursor.fetchall()


def format_report_rows(rows: List[Tuple]) -> List[str]:
    lines = ["Customer | Order | Product | Amount"]
    for row in rows:
        (
            customer_id,
            customer_name,
            city,
            state,
            order_id,
            order_date,
            status,
            product_id,
            product_name,
            quantity,
            unit_price,
            line_total,
            payment_amount,
            payment_method,
        ) = row
        lines.append(
            f"{order_date} | {order_id} | {customer_name} ({customer_id}) [{city}, {state}] | "
            f"{product_name} ({product_id}) x{quantity} @ ${unit_price:.2f} -> ${line_total:.2f} | "
            f"Payment ${payment_amount:.2f} via {payment_method} ({status})"
        )
    return lines


def rows_to_csv(rows: List[Tuple]) -> str:
    headers = [
        "customer_id",
        "customer_name",
        "city",
        "state",
        "order_id",
        "order_date",
        "status",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "line_total",
        "payment_amount",
        "payment_method",
    ]
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def save_report(rows: List[Tuple], fmt: str = "txt") -> str:
    ensure_reports_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if fmt == "csv":
        contents = rows_to_csv(rows)
        extension = "csv"
    else:
        contents = "\n".join(format_report_rows(rows))
        extension = "txt"
    report_path = REPORTS_DIR / f"report_{timestamp}.{extension}"
    report_path.write_text(contents, encoding="utf-8")
    return str(report_path)


def print_report_rows(limit: int = DEFAULT_REPORT_LIMIT, save_to_file: bool = True, fmt: str = "txt") -> None:
    rows = run_report(limit=limit)
    for line in format_report_rows(rows):
        print(line)
    if save_to_file:
        path = save_report(rows, fmt=fmt)
        print(f"\nReport saved to {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate multi-table e-commerce reports.")
    parser.add_argument("--limit", type=int, default=DEFAULT_REPORT_LIMIT, help="Number of rows to include.")
    parser.add_argument("--no-save", action="store_true", help="Print only; skip saving to file.")
    parser.add_argument("--format", choices=["txt", "csv"], default="txt", help="Report file format when saving.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print_report_rows(limit=args.limit, save_to_file=not args.no_save, fmt=args.format)

