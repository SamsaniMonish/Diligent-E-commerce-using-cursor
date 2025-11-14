import argparse
import csv
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from pipeline_utils import DATA_DIR, ensure_data_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce datasets.")
    parser.add_argument("--customers", type=int, default=150, help="Number of customers to generate.")
    parser.add_argument("--products", type=int, default=200, help="Number of products to generate.")
    parser.add_argument("--orders", type=int, default=300, help="Number of orders to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    return parser.parse_args()


def random_date_within_days(days_back: int = 365) -> str:
    start = datetime.now() - timedelta(days=days_back)
    rand_offset = random.randint(0, days_back)
    return (start + timedelta(days=rand_offset)).strftime("%Y-%m-%d")


def generate_customers(count: int = 150) -> List[Dict[str, str]]:
    first_names = ["Avery", "Jordan", "Logan", "Riley", "Parker", "Quinn", "Hayden", "Morgan", "Sawyer", "Dakota"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    cities = ["Austin", "Seattle", "Denver", "Boston", "Chicago", "Miami", "Atlanta", "Phoenix", "Portland", "Dallas"]
    states = ["TX", "WA", "CO", "MA", "IL", "FL", "GA", "AZ", "OR", "TX"]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]

    customers: List[Dict[str, str]] = []
    for i in range(1, count + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        customers.append(
            {
                "customer_id": f"CUST{i:04d}",
                "first_name": first,
                "last_name": last,
                "email": email,
                "city": random.choice(cities),
                "state": random.choice(states),
                "signup_date": random_date_within_days(730),
                "loyalty_tier": random.choices(tiers, weights=[0.45, 0.3, 0.2, 0.05])[0],
            }
        )
    return customers


def generate_products(count: int = 200) -> List[Dict[str, str]]:
    categories = {
        "Electronics": ["Bluetooth Speaker", "Wireless Earbuds", "Smart Watch", "Tablet", "Gaming Mouse"],
        "Home": ["Air Purifier", "Coffee Maker", "Blender", "Vacuum Cleaner", "Smart Lamp"],
        "Apparel": ["Running Shoes", "Denim Jacket", "Yoga Pants", "Baseball Cap", "Wool Scarf"],
        "Beauty": ["Moisturizer", "Serum", "Perfume", "Hair Dryer", "Shampoo"],
        "Outdoors": ["Camping Tent", "Hiking Backpack", "Travel Mug", "LED Lantern", "Water Bottle"],
    }

    products: List[Dict[str, str]] = []
    product_counter = 1
    for category, names in categories.items():
        for _ in range(count // len(categories)):
            name = random.choice(names)
            price = round(random.uniform(9.99, 399.99), 2)
            inventory = random.randint(50, 1000)
            products.append(
                {
                    "product_id": f"PROD{product_counter:04d}",
                    "name": f"{name} {random.randint(100, 999)}",
                    "category": category,
                    "price": price,
                    "inventory_count": inventory,
                }
            )
            product_counter += 1
    while len(products) < count:
        name = f"Accessory {len(products)+1}"
        products.append(
            {
                "product_id": f"PROD{product_counter:04d}",
                "name": name,
                "category": "Misc",
                "price": round(random.uniform(4.99, 49.99), 2),
                "inventory_count": random.randint(20, 500),
            }
        )
        product_counter += 1
    return products


def generate_orders(
    customers: List[Dict[str, str]],
    products: List[Dict[str, str]],
    count: int = 300,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    orders: List[Dict[str, str]] = []
    order_items: List[Dict[str, str]] = []
    payments: List[Dict[str, str]] = []

    for i in range(1, count + 1):
        customer = random.choice(customers)
        order_id = f"ORD{i:05d}"
        order_date = random_date_within_days(365)
        status = random.choices(["processing", "shipped", "delivered", "returned"], weights=[0.2, 0.3, 0.45, 0.05])[0]
        shipping_method = random.choice(["standard", "expedited", "overnight"])

        item_count = random.randint(1, 5)
        order_total = 0.0
        for item_index in range(1, item_count + 1):
            product = random.choice(products)
            quantity = random.randint(1, 5)
            unit_price = float(product["price"])
            line_total = round(quantity * unit_price, 2)
            order_total += line_total
            order_items.append(
                {
                    "order_item_id": f"ITEM{i:05d}_{item_index}",
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

        order_total = round(order_total, 2)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_date": order_date,
                "status": status,
                "shipping_method": shipping_method,
                "total_amount": order_total,
            }
        )

        payments.append(
            {
                "payment_id": f"PAY{i:05d}",
                "order_id": order_id,
                "payment_date": order_date,
                "amount": order_total,
                "method": random.choice(["credit_card", "paypal", "apple_pay", "google_pay"]),
                "status": "settled" if status != "returned" else "refunded",
                "transaction_id": f"TXN{i:010d}",
            }
        )

    return orders, order_items, payments


def write_csv(filename: str, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    filepath = DATA_DIR / filename
    with filepath.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def generate_and_save_data(
    customer_count: int = 150,
    product_count: int = 200,
    order_count: int = 300,
) -> Dict[str, List[Dict[str, str]]]:
    ensure_data_dir()
    customers = generate_customers(count=customer_count)
    products = generate_products(count=product_count)
    orders, order_items, payments = generate_orders(customers, products, count=order_count)

    datasets = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
    }

    for name, rows in datasets.items():
        write_csv(f"{name}.csv", rows)

    return datasets


if __name__ == "__main__":
    args = parse_args()
    random.seed(args.seed)
    generated = generate_and_save_data(
        customer_count=args.customers,
        product_count=args.products,
        order_count=args.orders,
    )
    for name, rows in generated.items():
        print(f"{name}: wrote {len(rows)} rows to {DATA_DIR / f'{name}.csv'} (seed={args.seed})")

