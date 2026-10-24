"""
Seed script for postgres-source -- one file, two modes.

    init  (default) -- the original full seed: customers, products and orders
          spread over the last DAYS_OF_HISTORY days, respecting foreign key
          order and the business rules baked into the schema
          (order_items.unit_price as a price snapshot, order_status_history
          as an append-only log maintained by application code, not a DB
          trigger). Fixed random seed -> reproducible dataset. Run once, on an
          EMPTY database (emails are UNIQUE, a second run would collide).

    daily -- appends ONE day of activity with today's timestamps, so the
          scheduled pipeline run gets real new data: new customers, a few
          customers changing country or email (new SCD2 versions), sometimes
          a product price change (new SCD2 version + new price snapshot in
          later orders), new orders (+ items, payments, status history) and
          lifecycle progress of open orders. History is never modified, only
          appended to -- backfill tests on past days stay valid. Idempotent
          per day: if today's batch already exists, it does nothing. Run once
          a day by the small `source_simulator` DAG, BEFORE the pipeline run;
          it is not part of the pipeline (in a company the source is someone
          else's application).

Connection settings come from environment variables with local defaults (same
as extract.py), so the script runs from the Mac (localhost) and from the
Airflow container (postgres-source). No Airflow imports (CLAUDE.md decision 8).

Usage:
    uv run python scripts/seed.py                  # init
    uv run python scripts/seed.py --mode daily     # one day of activity
"""

import argparse
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
from faker import Faker

# Random seeds are set per mode in main(): init = fixed 42 (reproducible
# dataset), daily = the date (reproducible per day, different every day).
fake = Faker()

# --- Connection settings: env vars, defaults = local docker-compose values ---
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_SOURCE_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_SOURCE_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_SOURCE_DB", "ecommerce"),
    "user": os.environ.get("POSTGRES_SOURCE_USER", "source_user"),
    "password": os.environ.get("POSTGRES_SOURCE_PASSWORD", "source_password"),
}

# --- Volume knobs -- tweak freely, kept small for a fast local seed ---
NUM_CUSTOMERS = 50
NUM_PRODUCTS = 30
NUM_ORDERS = 200
DAYS_OF_HISTORY = 90

PRODUCT_CATEGORIES = ["Electronics", "Home & Kitchen", "Books", "Sports", "Toys"]
ORDER_STATUS_FLOW = ["pending", "paid", "shipped", "delivered"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer"]

# --- Daily activity knobs (inclusive ranges) -- one simulated day ---
DAILY_NEW_CUSTOMERS = (1, 3)
DAILY_CUSTOMER_CHANGES = (1, 2)        # existing customers changing country or email -> new SCD2 version
DAILY_PRICE_CHANGE_PROBABILITY = 0.5   # one product changes price -> new SCD2 version
DAILY_NEW_ORDERS = (5, 15)
DAILY_SAME_DAY_PAYMENT_PROBABILITY = 0.6
DAILY_ORDER_TRANSITIONS = (3, 8)       # open orders moving one step along the lifecycle
DAILY_CANCEL_PROBABILITY = 0.1         # a pending order gets cancelled instead of paid
NEXT_STATUS = {"pending": "paid", "paid": "shipped", "shipped": "delivered"}


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


def seed_initial(cursor):
    print(f"Seeding {NUM_CUSTOMERS} customers...")
    customer_ids = seed_customers(cursor, NUM_CUSTOMERS)

    print(f"Seeding {NUM_PRODUCTS} products...")
    products = seed_products(cursor, NUM_PRODUCTS)

    print(f"Seeding {NUM_ORDERS} orders (+ items, payments, status history)...")
    seed_orders_with_history(cursor, customer_ids, products, NUM_ORDERS)


# ---------------------------------------------------------------------------
# daily mode -- one day of activity, timestamps between today 00:00 UTC and now
#
# Note on UPDATEs: the source tables have a BEFORE UPDATE trigger
# (set_updated_at in sql/ddl.sql) that sets updated_at = now(). The changed_at
# passed to UPDATE statements below is therefore overwritten: a changed row's
# updated_at (and so the SCD2 valid_from Spark derives from it) is the moment
# this script runs, like in a real application. The random times only take
# effect on INSERTs (new customers, orders, items, payments) and in
# order_status_history.changed_at, which has no trigger.
# ---------------------------------------------------------------------------

def todays_event_times(count: int, day_start: datetime, now: datetime) -> list[datetime]:
    """`count` random moments between today's 00:00 UTC and now, ascending."""
    span_seconds = max(int((now - day_start).total_seconds()), 1)
    return sorted(day_start + timedelta(seconds=random.randint(0, span_seconds - 1)) for _ in range(count))


def unused_email(cursor) -> str:
    """Random email not yet in customers (UNIQUE); sees this transaction's own inserts."""
    while True:
        email = fake.email()
        cursor.execute("SELECT 1 FROM customers WHERE email = %s", (email,))
        if cursor.fetchone() is None:
            return email


def today_already_generated(cursor, day_start: datetime) -> bool:
    """Every daily batch creates orders, so an order created today = batch done."""
    cursor.execute("SELECT EXISTS (SELECT 1 FROM orders WHERE created_at >= %s)", (day_start,))
    return cursor.fetchone()[0]


def add_new_customers(cursor, event_times: list[datetime]) -> list[tuple[int, datetime]]:
    new_customers = []
    for created_at in event_times:
        cursor.execute(
            """
            INSERT INTO customers (first_name, last_name, email, country, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (fake.first_name(), fake.last_name(), unused_email(cursor),
             fake.country_code(representation="alpha-2"), created_at, created_at),
        )
        new_customers.append((cursor.fetchone()[0], created_at))
    return new_customers


def change_existing_customers(cursor, existing_customer_ids: list[int], event_times: list[datetime]) -> int:
    """Each chosen customer changes country OR email once -> one new SCD2 version."""
    chosen_customers = random.sample(existing_customer_ids, k=min(len(event_times), len(existing_customer_ids)))
    for customer_id, changed_at in zip(chosen_customers, event_times):
        if random.random() < 0.5:
            cursor.execute("SELECT country FROM customers WHERE customer_id = %s", (customer_id,))
            current_country = cursor.fetchone()[0]
            new_country = current_country
            while new_country == current_country:
                new_country = fake.country_code(representation="alpha-2")
            cursor.execute(
                "UPDATE customers SET country = %s, updated_at = %s WHERE customer_id = %s",
                (new_country, changed_at, customer_id),
            )
        else:
            cursor.execute(
                "UPDATE customers SET email = %s, updated_at = %s WHERE customer_id = %s",
                (unused_email(cursor), changed_at, customer_id),
            )
    return len(chosen_customers)


def maybe_change_product_price(cursor, changed_at: datetime) -> int:
    """Sometimes one product changes price by 5-20%. Happens at the day's first
    event, so all of today's orders snapshot the new price (a simplification)."""
    if random.random() >= DAILY_PRICE_CHANGE_PROBABILITY:
        return 0
    cursor.execute("SELECT product_id, price FROM products ORDER BY product_id")
    product_id, current_price = random.choice(cursor.fetchall())
    new_price = round(float(current_price) * random.uniform(0.80, 1.20), 2)
    cursor.execute(
        "UPDATE products SET price = %s, updated_at = %s WHERE product_id = %s",
        (max(new_price, 0.0), changed_at, product_id),
    )
    return 1


def add_new_orders(cursor, existing_customer_ids: list[int], new_customers: list[tuple[int, datetime]],
                   event_times: list[datetime], now: datetime) -> int:
    """New orders placed today: items with the current price snapshot, status
    history, and for some of them a same-day payment (pending -> paid)."""
    cursor.execute("SELECT product_id, price FROM products ORDER BY product_id")
    products = [{"product_id": product_id, "price": float(price)} for product_id, price in cursor.fetchall()]

    for placed_at in event_times:
        # A customer created today can only order after their account exists.
        customers_existing_at_order_time = existing_customer_ids + [
            customer_id for customer_id, created_at in new_customers if created_at <= placed_at
        ]
        customer_id = random.choice(customers_existing_at_order_time)
        paid_at = placed_at + timedelta(minutes=random.randint(1, 120))
        is_paid_today = random.random() < DAILY_SAME_DAY_PAYMENT_PROBABILITY and paid_at <= now
        final_status = "paid" if is_paid_today else "pending"
        last_change_at = paid_at if is_paid_today else placed_at

        cursor.execute(
            """
            INSERT INTO orders (customer_id, status, order_date, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING order_id
            """,
            (customer_id, final_status, placed_at, placed_at, last_change_at),
        )
        order_id = cursor.fetchone()[0]

        order_line_items = random.sample(products, k=random.randint(1, 4))
        order_total = 0.0
        for product in order_line_items:
            qty = random.randint(1, 3)
            order_total += qty * product["price"]
            cursor.execute(
                """
                INSERT INTO order_items (order_id, product_id, qty, unit_price, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (order_id, product["product_id"], qty, product["price"], placed_at, placed_at),
            )

        cursor.execute(
            "INSERT INTO order_status_history (order_id, status, changed_at) VALUES (%s, %s, %s)",
            (order_id, "pending", placed_at),
        )
        if is_paid_today:
            cursor.execute(
                "INSERT INTO order_status_history (order_id, status, changed_at) VALUES (%s, %s, %s)",
                (order_id, "paid", paid_at),
            )
            cursor.execute(
                """
                INSERT INTO payments (order_id, amount, method, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (order_id, round(order_total, 2), random.choice(PAYMENT_METHODS), "completed", paid_at, paid_at),
            )
    return len(event_times)


def progress_open_orders(cursor, day_start: datetime, event_times: list[datetime]) -> int:
    """Orders placed before today move one step: pending -> paid (with payment)
    or cancelled, paid -> shipped, shipped -> delivered."""
    cursor.execute(
        "SELECT order_id, status FROM orders WHERE status IN ('pending', 'paid', 'shipped') "
        "AND created_at < %s ORDER BY order_id",
        (day_start,),
    )
    open_orders = cursor.fetchall()
    chosen_orders = random.sample(open_orders, k=min(len(event_times), len(open_orders)))
    for (order_id, current_status), changed_at in zip(chosen_orders, event_times):
        if current_status == "pending" and random.random() < DAILY_CANCEL_PROBABILITY:
            new_status = "cancelled"
        else:
            new_status = NEXT_STATUS[current_status]
        cursor.execute(
            "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
            (new_status, changed_at, order_id),
        )
        cursor.execute(
            "INSERT INTO order_status_history (order_id, status, changed_at) VALUES (%s, %s, %s)",
            (order_id, new_status, changed_at),
        )
        if new_status == "paid":
            cursor.execute(
                "SELECT COALESCE(SUM(qty * unit_price), 0) FROM order_items WHERE order_id = %s",
                (order_id,),
            )
            order_total = float(cursor.fetchone()[0])
            cursor.execute(
                """
                INSERT INTO payments (order_id, amount, method, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (order_id, round(order_total, 2), random.choice(PAYMENT_METHODS), "completed", changed_at, changed_at),
            )
    return len(chosen_orders)


def simulate_daily_activity(cursor) -> None:
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if today_already_generated(cursor, day_start):
        print(f"Done. Activity for {day_start:%Y-%m-%d} already generated -- nothing to do.")
        return

    cursor.execute("SELECT customer_id FROM customers WHERE created_at < %s ORDER BY customer_id", (day_start,))
    existing_customer_ids = [row[0] for row in cursor.fetchall()]

    price_changes = maybe_change_product_price(cursor, todays_event_times(1, day_start, now)[0])
    new_customers = add_new_customers(cursor, todays_event_times(random.randint(*DAILY_NEW_CUSTOMERS), day_start, now))
    customer_changes = change_existing_customers(
        cursor, existing_customer_ids, todays_event_times(random.randint(*DAILY_CUSTOMER_CHANGES), day_start, now)
    )
    new_orders = add_new_orders(
        cursor, existing_customer_ids, new_customers,
        todays_event_times(random.randint(*DAILY_NEW_ORDERS), day_start, now), now,
    )
    transitions = progress_open_orders(
        cursor, day_start, todays_event_times(random.randint(*DAILY_ORDER_TRANSITIONS), day_start, now)
    )
    print(
        f"Done. Activity for {day_start:%Y-%m-%d}: {len(new_customers)} new customers, "
        f"{customer_changes} customer changes, {price_changes} price changes, "
        f"{new_orders} new orders, {transitions} order status transitions."
    )


def main():
    parser = argparse.ArgumentParser(description="Seed postgres-source or simulate one day of activity.")
    parser.add_argument("--mode", choices=["init", "daily"], default="init",
                        help="init = full historical seed on an empty DB; daily = append today's activity")
    args = parser.parse_args()

    if args.mode == "init":
        Faker.seed(42)
        random.seed(42)
    else:
        day_number = datetime.now(timezone.utc).date().toordinal()
        Faker.seed(day_number)
        random.seed(day_number)

    connection = psycopg2.connect(**DB_CONFIG)
    try:
        # `with connection` = one transaction (commit on success, rollback on
        # error), so a failed run leaves nothing half-written.
        with connection, connection.cursor() as cursor:
            if args.mode == "init":
                seed_initial(cursor)
                print("Done. Data committed.")
            else:
                simulate_daily_activity(cursor)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
