"""
Seed script for postgres-source.

Populates all 6 tables defined in sql/ddl.sql with realistic, reproducible
fake data, respecting foreign key order and the business rules baked into
the schema (order_items.unit_price as a price snapshot, order_status_history
as an append-only log maintained by application code, not a DB trigger).

Usage:
    uv run python scripts/seed.py
"""

import random
from datetime import datetime, timedelta, timezone

import psycopg2
from faker import Faker

# --- Reproducibility: same seed -> same generated dataset every run ---
Faker.seed(42)
random.seed(42)
fake = Faker()

# --- Connection settings matching docker-compose.yml ---
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce",
    "user": "source_user",
    "password": "source_password",
}

# --- Volume knobs -- tweak freely, kept small for a fast local seed ---
NUM_CUSTOMERS = 50
NUM_PRODUCTS = 30
NUM_ORDERS = 200
DAYS_OF_HISTORY = 90

PRODUCT_CATEGORIES = ["Electronics", "Home & Kitchen", "Books", "Sports", "Toys"]
ORDER_STATUS_FLOW = ["pending", "paid", "shipped", "delivered"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer"]


def random_past_timestamp(days_back: int) -> datetime:
    """Random UTC timestamp somewhere in the last `days_back` days."""
    random_offset_seconds = random.randint(0, days_back * 24 * 3600)
    return datetime.now(timezone.utc) - timedelta(seconds=random_offset_seconds)


def seed_customers(cursor, num_customers: int) -> list[int]:
    customer_ids = []
    for _ in range(num_customers):
        created_at = random_past_timestamp(DAYS_OF_HISTORY)
        cursor.execute(
            """
            INSERT INTO customers (first_name, last_name, email, country, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.country_code(representation="alpha-2"),
                created_at,
                created_at,  # matches created_at on insert -- no update has happened yet
            ),
        )
        customer_ids.append(cursor.fetchone()[0])
    return customer_ids


def seed_products(cursor, num_products: int) -> list[dict]:
    products = []
    for _ in range(num_products):
        created_at = random_past_timestamp(DAYS_OF_HISTORY)
        price = round(random.uniform(5.0, 500.0), 2)
        cursor.execute(
            """
            INSERT INTO products (name, category, price, stock_qty, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING product_id
            """,
            (
                fake.catch_phrase(),
                random.choice(PRODUCT_CATEGORIES),
                price,
                random.randint(0, 500),
                created_at,
                created_at,
            ),
        )
        product_id = cursor.fetchone()[0]
        products.append({"product_id": product_id, "price": price})
    return products


def seed_orders_with_history(cursor, customer_ids: list[int], products: list[dict], num_orders: int):
    """
    For each order: insert the order itself, 1-4 order_items (price snapshot
    from the product's CURRENT price at order time), one payment, and a full
    order_status_history trail up to wherever the order's lifecycle stopped
    (some orders are deliberately left mid-flow or cancelled, like real data).
    """
    for _ in range(num_orders):
        order_placed_at = random_past_timestamp(DAYS_OF_HISTORY)
        customer_id = random.choice(customer_ids)

        # Decide how far this order's lifecycle progressed -- most complete
        # the full flow, a few get cancelled partway, mirroring real traffic.
        if random.random() < 0.08:
            status_history = ["pending", "cancelled"]
        else:
            progressed_to = random.randint(1, len(ORDER_STATUS_FLOW))
            status_history = ORDER_STATUS_FLOW[:progressed_to]

        final_status = status_history[-1]

        cursor.execute(
            """
            INSERT INTO orders (customer_id, status, order_date, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING order_id
            """,
            (customer_id, final_status, order_placed_at, order_placed_at, order_placed_at),
        )
        order_id = cursor.fetchone()[0]

        # order_status_history -- append-only, one row per transition,
        # timestamps spaced a few hours apart after the order was placed.
        status_changed_at = order_placed_at
        for status in status_history:
            cursor.execute(
                """
                INSERT INTO order_status_history (order_id, status, changed_at)
                VALUES (%s, %s, %s)
                """,
                (order_id, status, status_changed_at),
            )
            status_changed_at += timedelta(hours=random.randint(2, 48))

        # order_items -- unit_price is a SNAPSHOT of the product's price
        # at order time, deliberately not a live reference to products.price.
        order_line_items = random.sample(products, k=random.randint(1, 4))
        order_total = 0.0
        for product in order_line_items:
            qty = random.randint(1, 3)
            unit_price = product["price"]
            order_total += qty * unit_price
            cursor.execute(
                """
                INSERT INTO order_items (order_id, product_id, qty, unit_price, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (order_id, product["product_id"], qty, unit_price, order_placed_at, order_placed_at),
            )

        # payments -- only orders that reached at least "paid" have a
        # completed payment; cancelled-while-pending orders have none.
        if final_status != "pending" or "cancelled" in status_history:
            payment_status = "refunded" if final_status == "cancelled" and "paid" in status_history else (
                "failed" if final_status == "cancelled" else "completed"
            )
            cursor.execute(
                """
                INSERT INTO payments (order_id, amount, method, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    round(order_total, 2),
                    random.choice(PAYMENT_METHODS),
                    payment_status,
                    order_placed_at,
                    order_placed_at,
                ),
            )


def main():
    connection = psycopg2.connect(**DB_CONFIG)
    try:
        with connection:
            with connection.cursor() as cursor:
                print(f"Seeding {NUM_CUSTOMERS} customers...")
                customer_ids = seed_customers(cursor, NUM_CUSTOMERS)

                print(f"Seeding {NUM_PRODUCTS} products...")
                products = seed_products(cursor, NUM_PRODUCTS)

                print(f"Seeding {NUM_ORDERS} orders (+ items, payments, status history)...")
                seed_orders_with_history(cursor, customer_ids, products, NUM_ORDERS)

        print("Done. Data committed.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
