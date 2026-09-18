-- =====================================================================
-- Schema for the SOURCE database (postgres-source) -- simulates an
-- e-commerce OLTP system. Postgres runs this file automatically on the
-- first container startup (docker-entrypoint-initdb.d), see docker-compose.yml.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Function + trigger: automatically sets updated_at on EVERY UPDATE.
-- This is the foundation for watermark-based incremental extraction
-- (WHERE updated_at > last_watermark) -- without it we'd have to
-- remember to set updated_at manually on every UPDATE, easy to miss.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------
-- customers -- a typical "has state that changes" entity -> dominant
-- pattern is UPDATE (customer changes address/country), not INSERT.
-- ---------------------------------------------------------------------
CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) NOT NULL UNIQUE,
    country         VARCHAR(2)   NOT NULL,   -- ISO code, e.g. 'PL', 'DE' -- see the "wow moment" from ADR-002
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_customers_updated_at ON customers (updated_at);

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------
-- products -- stock_qty changes continuously with every sale.
-- ---------------------------------------------------------------------
CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    price           NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    stock_qty       INTEGER NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_products_updated_at ON products (updated_at);

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------
-- orders -- INSERT (new order) + UPDATE (status change). Still the
-- SAME row throughout the order's entire lifecycle.
-- ---------------------------------------------------------------------
CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    order_date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_updated_at ON orders (updated_at);
CREATE INDEX idx_orders_customer_id ON orders (customer_id);

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------
-- order_items -- mostly INSERT, rarely changes after the fact.
--
-- IMPORTANT: unit_price is DELIBERATELY a snapshot of the price AT THE
-- TIME of the order, NOT a live join to products.price. If products.price
-- changed a month later, past orders must NOT change their value --
-- a fundamental OLTP rule for transactional data (see cheat sheet 10.1:
-- a derived value in general, but here specifically a HISTORICAL price
-- that must stay frozen, not recalculated).
-- ---------------------------------------------------------------------
CREATE TABLE order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    qty             INTEGER NOT NULL CHECK (qty > 0),
    unit_price      NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),  -- snapshot, not an FK to products.price
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_items_updated_at ON order_items (updated_at);
CREATE INDEX idx_order_items_order_id ON order_items (order_id);

CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON order_items
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------
-- payments -- a payment tied to an order, with its own status
-- (evolves independently from orders.status).
-- ---------------------------------------------------------------------
CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    amount          NUMERIC(10, 2) NOT NULL CHECK (amount >= 0),
    method          VARCHAR(20) NOT NULL
                        CHECK (method IN ('credit_card', 'debit_card', 'paypal', 'bank_transfer')),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'completed', 'failed', 'refunded')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_payments_updated_at ON payments (updated_at);
CREATE INDEX idx_payments_order_id ON payments (order_id);

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------
-- order_status_history -- a pure append-only event log. CONTRASTS with
-- orders.status (mutable -- a single column overwritten by UPDATE):
-- here EVERY status change is a NEW row, nothing is ever overwritten or
-- deleted. This is your "insert-only" example next to "update-in-place"
-- in the same schema -- a good interview talking point on state vs.
-- event history.
--
-- No updated_at/trigger -- this pattern by definition never does an
-- UPDATE, so the incremental extract watermark here is created_at.
-- ---------------------------------------------------------------------
CREATE TABLE order_status_history (
    history_id      SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    status          VARCHAR(20) NOT NULL
                        CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_status_history_changed_at ON order_status_history (changed_at);
CREATE INDEX idx_order_status_history_order_id ON order_status_history (order_id);
