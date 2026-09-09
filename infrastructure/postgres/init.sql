-- =============================================================================
-- Financial Risk & Fraud Detection Platform — PostgreSQL Schema
-- This is the OLTP source database (simulates a financial company's backend)
-- =============================================================================

SET client_encoding = 'UTF8';

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Enums ─────────────────────────────────────────────────────────────────────
CREATE TYPE account_type_enum    AS ENUM ('checking', 'savings', 'credit', 'loan', 'investment');
CREATE TYPE account_status_enum  AS ENUM ('active', 'inactive', 'suspended', 'closed');
CREATE TYPE customer_segment_enum AS ENUM ('retail', 'premium', 'business', 'vip', 'student');
CREATE TYPE merchant_risk_enum   AS ENUM ('low', 'medium', 'high');
CREATE TYPE device_type_enum     AS ENUM ('mobile', 'desktop', 'tablet', 'atm', 'pos_terminal');
CREATE TYPE transaction_type_enum AS ENUM ('purchase', 'withdrawal', 'transfer', 'refund', 'payment', 'deposit');
CREATE TYPE transaction_status_enum AS ENUM ('pending', 'completed', 'failed', 'reversed', 'flagged');
CREATE TYPE payment_method_enum  AS ENUM ('card', 'bank_transfer', 'wallet', 'upi', 'crypto', 'cheque');
CREATE TYPE event_type_enum      AS ENUM (
    'transaction_created', 'payment_attempted', 'payment_failed',
    'payment_completed', 'transaction_reversed', 'fraud_flagged',
    'review_initiated', 'review_completed'
);
CREATE TYPE currency_enum AS ENUM (
    'USD', 'EUR', 'GBP', 'INR', 'JPY', 'CAD', 'AUD', 'SGD',
    'CHF', 'CNY', 'HKD', 'BRL', 'MXN', 'AED', 'SAR'
);

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- ── Customers ─────────────────────────────────────────────────────────────────
CREATE TABLE customers (
    customer_id       UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    name              VARCHAR(255)  NOT NULL,
    email             VARCHAR(320)  NOT NULL UNIQUE,
    phone             VARCHAR(30),
    country           CHAR(2)       NOT NULL,  -- ISO 3166-1 alpha-2
    city              VARCHAR(100),
    customer_segment  customer_segment_enum NOT NULL DEFAULT 'retail',
    date_of_birth     DATE,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customers_country ON customers(country);
CREATE INDEX idx_customers_segment ON customers(customer_segment);
CREATE INDEX idx_customers_created ON customers(created_at);

-- ── Accounts ──────────────────────────────────────────────────────────────────
CREATE TABLE accounts (
    account_id      UUID               PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id     UUID               NOT NULL REFERENCES customers(customer_id),
    account_type    account_type_enum  NOT NULL,
    account_status  account_status_enum NOT NULL DEFAULT 'active',
    currency        currency_enum      NOT NULL DEFAULT 'USD',
    opened_at       TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ        NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_closed_after_opened CHECK (closed_at IS NULL OR closed_at > opened_at)
);

CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_accounts_status   ON accounts(account_status);

-- ── Merchants ─────────────────────────────────────────────────────────────────
CREATE TABLE merchants (
    merchant_id       UUID               PRIMARY KEY DEFAULT uuid_generate_v4(),
    merchant_name     VARCHAR(255)       NOT NULL,
    merchant_category VARCHAR(100)       NOT NULL,  -- MCC description
    mcc_code          CHAR(4),                      -- ISO 18245 Merchant Category Code
    country           CHAR(2)            NOT NULL,
    city              VARCHAR(100),
    risk_category     merchant_risk_enum NOT NULL DEFAULT 'low',
    created_at        TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_merchants_category ON merchants(merchant_category);
CREATE INDEX idx_merchants_risk     ON merchants(risk_category);
CREATE INDEX idx_merchants_country  ON merchants(country);

-- ── Devices ───────────────────────────────────────────────────────────────────
CREATE TABLE devices (
    device_id      UUID             PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id    UUID             NOT NULL REFERENCES customers(customer_id),
    device_type    device_type_enum NOT NULL,
    os             VARCHAR(50),
    browser        VARCHAR(100),
    ip_address     INET,
    user_agent     TEXT,
    first_seen_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    last_seen_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_devices_customer    ON devices(customer_id);
CREATE INDEX idx_devices_ip          ON devices(ip_address);
CREATE INDEX idx_devices_last_seen   ON devices(last_seen_at);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ── Transactions ──────────────────────────────────────────────────────────────
CREATE TABLE transactions (
    transaction_id          UUID                    PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id             UUID                    NOT NULL REFERENCES customers(customer_id),
    account_id              UUID                    NOT NULL REFERENCES accounts(account_id),
    merchant_id             UUID                    REFERENCES merchants(merchant_id),
    device_id               UUID                    REFERENCES devices(device_id),
    transaction_timestamp   TIMESTAMPTZ             NOT NULL,
    amount                  NUMERIC(18, 4)          NOT NULL,
    currency                currency_enum           NOT NULL DEFAULT 'USD',
    amount_usd              NUMERIC(18, 4),          -- normalized for comparison
    transaction_type        transaction_type_enum   NOT NULL,
    country                 CHAR(2)                 NOT NULL,
    city                    VARCHAR(100),
    payment_method          payment_method_enum     NOT NULL,
    status                  transaction_status_enum NOT NULL DEFAULT 'pending',
    ip_address              INET,
    is_international        BOOLEAN                 NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_amount_positive CHECK (amount > 0)
);

-- Partitioned by month for performance (in PostgreSQL declarative partitioning)
CREATE INDEX idx_txn_customer       ON transactions(customer_id);
CREATE INDEX idx_txn_account        ON transactions(account_id);
CREATE INDEX idx_txn_merchant       ON transactions(merchant_id);
CREATE INDEX idx_txn_device         ON transactions(device_id);
CREATE INDEX idx_txn_timestamp      ON transactions(transaction_timestamp DESC);
CREATE INDEX idx_txn_status         ON transactions(status);
CREATE INDEX idx_txn_country        ON transactions(country);

-- ── Transaction Events ────────────────────────────────────────────────────────
CREATE TABLE transaction_events (
    event_id                UUID           PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id          UUID           NOT NULL REFERENCES transactions(transaction_id),
    event_type              event_type_enum NOT NULL,
    event_timestamp         TIMESTAMPTZ    NOT NULL,
    event_metadata          JSONB,          -- flexible payload per event type
    created_at              TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_transaction  ON transaction_events(transaction_id);
CREATE INDEX idx_events_type         ON transaction_events(event_type);
CREATE INDEX idx_events_timestamp    ON transaction_events(event_timestamp DESC);

-- =============================================================================
-- AUDIT / METADATA
-- =============================================================================

-- Track ingestion batches for incremental loading
CREATE TABLE ingestion_watermarks (
    table_name       VARCHAR(100)  PRIMARY KEY,
    last_loaded_at   TIMESTAMPTZ   NOT NULL,
    rows_loaded      BIGINT        NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

INSERT INTO ingestion_watermarks (table_name, last_loaded_at, rows_loaded)
VALUES
    ('customers',           '1970-01-01 00:00:00+00', 0),
    ('accounts',            '1970-01-01 00:00:00+00', 0),
    ('merchants',           '1970-01-01 00:00:00+00', 0),
    ('devices',             '1970-01-01 00:00:00+00', 0),
    ('transactions',        '1970-01-01 00:00:00+00', 0),
    ('transaction_events',  '1970-01-01 00:00:00+00', 0);

-- =============================================================================
-- TRIGGERS — auto-update updated_at columns
-- =============================================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_merchants_updated_at
    BEFORE UPDATE ON merchants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
