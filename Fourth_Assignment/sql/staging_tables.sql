CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    signup_date DATE,
    country VARCHAR(50),
    load_date DATE
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id INT,
    product_name VARCHAR(200),
    category VARCHAR(100),
    unit_price NUMERIC(10,2),
    load_date DATE
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id INT,
    order_timestamp TIMESTAMP,
    customer_id INT,
    product_id INT,
    quantity INT,
    total_amount NUMERIC(10,2),
    currency VARCHAR(10),
    status VARCHAR(50),
    load_date DATE
);

