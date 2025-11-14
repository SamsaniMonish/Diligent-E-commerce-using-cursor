import sqlite3
from datetime import datetime
from typing import List, Tuple

from pipeline_utils import DB_PATH, REPORTS_DIR, ensure_reports_dir


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


def save_report(lines: List[str]) -> str:
    ensure_reports_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"report_{timestamp}.txt"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return str(report_path)


def print_report_rows(limit: int = 20, save_to_file: bool = True) -> None:
    rows = run_report(limit=limit)
    lines = format_report_rows(rows)
    for line in lines:
        print(line)
    if save_to_file:
        path = save_report(lines)
        print(f"\nReport saved to {path}")


if __name__ == "__main__":
    print_report_rows(limit=20)

