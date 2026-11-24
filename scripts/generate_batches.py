"""Generate deterministic incremental e-commerce batches into a Unity Catalog Volume.

Usage:
    python scripts/generate_batches.py --batch 1
    python scripts/generate_batches.py --batch 2
    python scripts/generate_batches.py --batch 3
    python scripts/generate_batches.py --batch 1 --catalog workspace
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from faker import Faker # type: ignore

BASE_TIME = datetime(2026, 11, 1, 9, 0, 0, tzinfo=timezone.utc)
CATEGORIES = ["Electronics", "Home & Kitchen", "Apparel", "Books", "Fitness", "Accessories"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
ORDER_STATUSES = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED"]
PROMO_CODES = ["SAVE10", "WELCOME15", "FESTIVE20", None, None]
PAYMENT_CHANNELS = ["CARD", "UPI", "NETBANKING", "WALLET"]


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_batch_1_customers(count: int = 500) -> list[dict[str, Any]]:
    fake = Faker()
    fake.seed_instance(101)
    rng = random.Random(101)
    rows: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        rows.append(
            {
                "customer_id": f"CUST-{i:05d}",
                "full_name": fake.name(),
                "email": fake.unique.email(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "loyalty_tier": rng.choices(LOYALTY_TIERS, weights=[45, 30, 18, 7], k=1)[0],
                "updated_at": _iso(BASE_TIME - timedelta(days=rng.randint(1, 30))),
            }
        )
    return rows


def build_batch_1_products(count: int = 100) -> list[dict[str, Any]]:
    fake = Faker()
    fake.seed_instance(202)
    rng = random.Random(202)
    rows: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        category = rng.choice(CATEGORIES)
        rows.append(
            {
                "product_id": f"PROD-{i:04d}",
                "product_name": f"{fake.word().title()} {category.split()[0]} Item {i}",
                "category": category,
                "unit_price": round(rng.uniform(10.0, 500.0), 2),
                "in_stock": rng.random() > 0.08,
                "updated_at": _iso(BASE_TIME - timedelta(days=rng.randint(1, 30))),
            }
        )
    return rows


def build_batch_1_orders(
    customers: list[dict[str, Any]],
    products: list[dict[str, Any]],
    count: int = 2000,
) -> list[dict[str, Any]]:
    rng = random.Random(303)
    rows: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        cust = rng.choice(customers)
        prod = rng.choice(products)
        order_ts = BASE_TIME + timedelta(minutes=rng.randint(0, 1440))
        rows.append(
            {
                "order_id": f"ORD-{i:06d}",
                "customer_id": cust["customer_id"],
                "product_id": prod["product_id"],
                "quantity": rng.randint(1, 5),
                "unit_price": prod["unit_price"],
                "order_status": rng.choice(ORDER_STATUSES),
                "order_timestamp": _iso(order_ts),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_ndjson(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write newline-delimited JSON (native format for Spark Auto Loader)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def generate_batch_1(landing_dir: Path) -> None:
    customers = build_batch_1_customers()
    products = build_batch_1_products()
    orders = build_batch_1_orders(customers, products)

    write_csv(landing_dir / "customers" / "customers_batch_01.csv", customers)
    write_ndjson(landing_dir / "products" / "products_batch_01.json", products)
    write_ndjson(landing_dir / "orders" / "orders_batch_01.json", orders)

    print("Batch 1 written (clean baseline):")
    print(f"  customers_batch_01.csv -> {len(customers)} rows")
    print(f"  products_batch_01.json -> {len(products)} rows")
    print(f"  orders_batch_01.json   -> {len(orders)} rows")


def generate_batch_2(landing_dir: Path) -> None:
    base_customers = build_batch_1_customers()
    base_products = build_batch_1_products()
    base_orders = build_batch_1_orders(base_customers, base_products)

    batch_2_time = BASE_TIME + timedelta(days=2)
    fake = Faker()
    fake.seed_instance(404)
    rng = random.Random(404)

    # 1. Customers: 50 new + 30 SCD Type 2 updates (city/state/tier changed)
    new_customers = [
        {
            "customer_id": f"CUST-{i:05d}",
            "full_name": fake.name(),
            "email": fake.unique.email(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "loyalty_tier": rng.choice(LOYALTY_TIERS),
            "updated_at": _iso(batch_2_time),
        }
        for i in range(501, 551)
    ]
    updated_customers: list[dict[str, Any]] = []
    for cust in base_customers[:30]:
        next_tier_idx = min(LOYALTY_TIERS.index(cust["loyalty_tier"]) + 1, len(LOYALTY_TIERS) - 1)
        updated_customers.append(
            {
                **cust,
                "city": fake.city(),
                "state": fake.state_abbr(),
                "loyalty_tier": LOYALTY_TIERS[next_tier_idx],
                "updated_at": _iso(batch_2_time + timedelta(hours=1)),
            }
        )
    batch_2_customers = new_customers + updated_customers

    # 2. Products: 15 new + 10 price updates
    new_products = [
        {
            "product_id": f"PROD-{i:04d}",
            "product_name": f"{fake.word().title()} Product {i}",
            "category": rng.choice(CATEGORIES),
            "unit_price": round(rng.uniform(15.0, 450.0), 2),
            "in_stock": True,
            "updated_at": _iso(batch_2_time),
        }
        for i in range(101, 116)
    ]
    updated_products = [
        {
            **prod,
            "unit_price": round(prod["unit_price"] * 1.10, 2),
            "updated_at": _iso(batch_2_time + timedelta(hours=1)),
        }
        for prod in base_products[:10]
    ]
    batch_2_products = new_products + updated_products

    # 3. Orders: 600 new + 25 exact duplicates + 15 status updates
    all_customers = base_customers + new_customers
    all_products = base_products + new_products
    new_orders: list[dict[str, Any]] = []
    for i in range(2001, 2601):
        cust = rng.choice(all_customers)
        prod = rng.choice(all_products)
        order_ts = batch_2_time + timedelta(minutes=rng.randint(0, 1440))
        new_orders.append(
            {
                "order_id": f"ORD-{i:06d}",
                "customer_id": cust["customer_id"],
                "product_id": prod["product_id"],
                "quantity": rng.randint(1, 5),
                "unit_price": prod["unit_price"],
                "order_status": rng.choice(ORDER_STATUSES),
                "order_timestamp": _iso(order_ts),
            }
        )

    exact_duplicates = [dict(row) for row in base_orders[:25]]
    status_updates = [
        {
            **row,
            "order_status": "DELIVERED",
            "order_timestamp": _iso(batch_2_time + timedelta(hours=2)),
        }
        for row in base_orders[25:40]
    ]
    batch_2_orders = new_orders + exact_duplicates + status_updates

    write_csv(landing_dir / "customers" / "customers_batch_02.csv", batch_2_customers)
    write_ndjson(landing_dir / "products" / "products_batch_02.json", batch_2_products)
    write_ndjson(landing_dir / "orders" / "orders_batch_02.json", batch_2_orders)

    print("Batch 2 written (updates + duplicates):")
    print(f"  customers_batch_02.csv -> {len(new_customers)} new, {len(updated_customers)} SCD2 updates")
    print(f"  products_batch_02.json -> {len(new_products)} new, {len(updated_products)} price updates")
    print(
        f"  orders_batch_02.json   -> {len(new_orders)} new, "
        f"{len(exact_duplicates)} exact duplicates, {len(status_updates)} status updates"
    )


def generate_batch_3(landing_dir: Path) -> None:
    base_customers = build_batch_1_customers()
    base_products = build_batch_1_products()

    batch_3_time = BASE_TIME + timedelta(days=4)
    rng = random.Random(505)

    # 400 valid orders with schema drift (two unexpected columns: promo_code, payment_channel)
    drifted_orders: list[dict[str, Any]] = []
    for i in range(2601, 3001):
        cust = rng.choice(base_customers)
        prod = rng.choice(base_products)
        order_ts = batch_3_time + timedelta(minutes=rng.randint(0, 1440))
        drifted_orders.append(
            {
                "order_id": f"ORD-{i:06d}",
                "customer_id": cust["customer_id"],
                "product_id": prod["product_id"],
                "quantity": rng.randint(1, 4),
                "unit_price": prod["unit_price"],
                "order_status": rng.choice(ORDER_STATUSES),
                "order_timestamp": _iso(order_ts),
                "promo_code": rng.choice(PROMO_CODES),
                "payment_channel": rng.choice(PAYMENT_CHANNELS),
            }
        )

    # 20 intentionally malformed rows to verify Silver quarantine rules
    bad_orders: list[dict[str, Any]] = []
    for idx in range(1, 6):
        bad_orders.append(
            {
                "order_id": None,  # Null primary key
                "customer_id": f"CUST-{idx:05d}",
                "product_id": f"PROD-{idx:04d}",
                "quantity": 1,
                "unit_price": 25.00,
                "order_status": "PENDING",
                "order_timestamp": _iso(batch_3_time),
            }
        )
    for idx in range(6, 11):
        bad_orders.append(
            {
                "order_id": f"ORD-BADQTY-{idx:02d}",
                "customer_id": f"CUST-{idx:05d}",
                "product_id": f"PROD-{idx:04d}",
                "quantity": -2,  # Invalid negative quantity
                "unit_price": 30.00,
                "order_status": "PENDING",
                "order_timestamp": _iso(batch_3_time),
            }
        )
    for idx in range(11, 16):
        bad_orders.append(
            {
                "order_id": f"ORD-BADPRC-{idx:02d}",
                "customer_id": f"CUST-{idx:05d}",
                "product_id": f"PROD-{idx:04d}",
                "quantity": 2,
                "unit_price": -49.99,  # Invalid negative price
                "order_status": "PROCESSING",
                "order_timestamp": _iso(batch_3_time),
            }
        )
    for idx in range(16, 21):
        bad_orders.append(
            {
                "order_id": f"ORD-ORPHAN-{idx:02d}",
                "customer_id": "CUST-99999",  # Orphan foreign key
                "product_id": f"PROD-{idx:04d}",
                "quantity": 1,
                "unit_price": 75.00,
                "order_status": "SHIPPED",
                "order_timestamp": _iso(batch_3_time),
            }
        )

    all_batch_3 = drifted_orders + bad_orders
    write_ndjson(landing_dir / "orders" / "orders_batch_03.json", all_batch_3)

    print("Batch 3 written (schema drift + bad records):")
    print(f"  orders_batch_03.json -> {len(drifted_orders)} schema-drifted orders, {len(bad_orders)} bad rows")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate incremental e-commerce batches.")
    parser.add_argument("--batch", type=int, choices=[1, 2, 3], required=True, help="Batch number to generate")
    parser.add_argument(
        "--catalog",
        type=str,
        default="ecommerce_lakehouse",
        help="Unity Catalog name (default: ecommerce_lakehouse)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional local override path instead of /Volumes/<catalog>/raw/landing",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    landing_dir = args.output_dir or Path(f"/Volumes/{args.catalog}/raw/landing")

    if not landing_dir.exists():
        raise FileNotFoundError(
            f"Landing path not found: {landing_dir}. Confirm your Unity Catalog Volume exists "
            f"(or pass --catalog workspace if you used the default catalog)."
        )

    generators = {1: generate_batch_1, 2: generate_batch_2, 3: generate_batch_3}
    generators[args.batch](landing_dir)


if __name__ == "__main__":
    main()