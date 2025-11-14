import csv
import sqlite3

from pipeline_utils import DATA_DIR, DB_PATH


def reset_database(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA foreign_keys = OFF;")
    tables = ["order_items", "payments", "orders", "products", "customers"]
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
    cursor.execute("PRAGMA foreign_keys = ON;")


def create_tables(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE customers (
            customer_id TEXT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            city TEXT,
            state TEXT,
            signup_date TEXT,
            loyalty_tier TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price REAL NOT NULL,
            inventory_count INTEGER NOT NULL
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT,
            shipping_method TEXT,
            total_amount REAL NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE order_items (
            order_item_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE payments (
            payment_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            payment_date TEXT NOT NULL,
            amount REAL NOT NULL,
            method TEXT NOT NULL,
            status TEXT NOT NULL,
            transaction_id TEXT NOT NULL UNIQUE,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );
        """
    )


def load_csv_into_table(cursor: sqlite3.Cursor, filename: str, table: str) -> None:
    filepath = DATA_DIR / filename
    with filepath.open("r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        columns = reader.fieldnames or []
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        for row in rows:
            values = [row[col] for col in columns]
            cursor.execute(f"INSERT INTO {table} ({column_list}) VALUES ({placeholders});", values)


def load_data_into_sqlite() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        reset_database(cursor)
        create_tables(cursor)
        conn.commit()

        load_csv_into_table(cursor, "customers.csv", "customers")
        load_csv_into_table(cursor, "products.csv", "products")
        load_csv_into_table(cursor, "orders.csv", "orders")
        load_csv_into_table(cursor, "order_items.csv", "order_items")
        load_csv_into_table(cursor, "payments.csv", "payments")
        conn.commit()


if __name__ == "__main__":
    load_data_into_sqlite()
    print(f"Loaded CSV data into {DB_PATH}")

