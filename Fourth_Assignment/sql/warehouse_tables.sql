CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    signup_date DATE,
    country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    unit_price NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INT PRIMARY KEY,
    order_timestamp_utc TIMESTAMP,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_amount_usd NUMERIC(10,2),
    currency VARCHAR(10),
    currency_mismatch_flag BOOLEAN,
    status VARCHAR(20),
    order_date DATE
);
