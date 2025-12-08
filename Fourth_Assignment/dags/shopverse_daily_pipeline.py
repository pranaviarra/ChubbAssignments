import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
import json

default_args = {
    "owner": "shopverse_de",
    "retries": 1,
    "email_on_failure": True,
    "email": "alert@example.com"
}

@dag(
    dag_id="shopverse_daily_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
    catchup=True,
    default_args=default_args,
    tags=["shopverse"]
)
def shopverse_daily_pipeline():

    # ---- Base path from Airflow Variable ---- #
    base_path = Variable.get("shopverse_data_base_path")
    execution_date = "{{ ds_nodash }}"

    customer_file = f"{base_path}/landing/customers/customers_{execution_date}.csv"
    product_file = f"{base_path}/landing/products/products_{execution_date}.csv"
    order_file = f"{base_path}/landing/orders/orders_{execution_date}.json"

    # ---- Sensors ---- #
    wait_customers = FileSensor(
        task_id="wait_customers_file",
        filepath=customer_file,
        poke_interval=10,
        timeout=600
    )

    wait_products = FileSensor(
        task_id="wait_products_file",
        filepath=product_file,
        poke_interval=10,
        timeout=600
    )

    wait_orders = FileSensor(
        task_id="wait_orders_file",
        filepath=order_file,
        poke_interval=10,
        timeout=600
    )

    # ---- STAGING LAYER ---- #
    with TaskGroup("staging") as staging:

        @task
        def load_customers(file_path: str):
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            hook.run("TRUNCATE TABLE stg_customers")

            with open(file_path, "r") as f:
                next(f)
                for row in f:
                    cols = row.strip().split(",")
                    hook.run("""
                        INSERT INTO stg_customers 
                        (customer_id, first_name, last_name, email, signup_date, country, load_date)
                        VALUES (%s,%s,%s,%s,%s,%s,current_date)
                    """, parameters=cols)

        @task
        def load_products(file_path: str):
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            hook.run("TRUNCATE TABLE stg_products")

            with open(file_path, "r") as f:
                next(f)
                for row in f:
                    cols = row.strip().split(",")
                    hook.run("""
                        INSERT INTO stg_products
                        (product_id, product_name, category, unit_price, load_date)
                        VALUES (%s,%s,%s,%s,current_date)
                    """, parameters=cols)

        @task
        def load_orders(file_path: str):
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            hook.run("TRUNCATE TABLE stg_orders")

            with open(file_path, "r") as f:
                orders = json.load(f)
                for o in orders:
                    hook.run("""
                        INSERT INTO stg_orders
                        (order_id, order_timestamp, customer_id, product_id, quantity, total_amount,
                         currency, status, load_date)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,current_date)
                    """, parameters=[
                        o["order_id"], o["order_timestamp"], o["customer_id"],
                        o["product_id"], o["quantity"], o["total_amount"],
                        o["currency"], o["status"]
                    ])

        load_customers(customer_file)
        load_products(product_file)
        load_orders(order_file)

    # ---- WAREHOUSE TRANSFORM ---- #
    with TaskGroup("warehouse") as warehouse:

        @task
        def refresh_dim_customers():
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            hook.run("TRUNCATE TABLE dim_customers")
            hook.run("""
                INSERT INTO dim_customers (customer_id, first_name, last_name, email, signup_date, country)
                SELECT DISTINCT customer_id, first_name, last_name, email, signup_date, country
                FROM stg_customers
            """)

        @task
        def refresh_dim_products():
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            hook.run("TRUNCATE TABLE dim_products")
            hook.run("""
                INSERT INTO dim_products (product_id, product_name, category, unit_price)
                SELECT DISTINCT product_id, product_name, category, unit_price
                FROM stg_products
            """)

        @task
        def refresh_fact_orders():
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            hook.run("DELETE FROM fact_orders WHERE order_date = CURRENT_DATE")

            hook.run("""
                INSERT INTO fact_orders (
                    order_id,
                    order_timestamp_utc,
                    customer_id,
                    product_id,
                    quantity,
                    total_amount_usd,
                    currency,
                    currency_mismatch_flag,
                    status,
                    order_date
                )
                SELECT
                    o.order_id,
                    (o.order_timestamp AT TIME ZONE 'UTC'),
                    o.customer_id,
                    o.product_id,
                    o.quantity,
                    CASE WHEN o.currency = 'USD'
                         THEN o.total_amount
                         ELSE o.total_amount * 0.012 END,
                    o.currency,
                    CASE WHEN o.currency <> 'USD' THEN TRUE ELSE FALSE END,
                    o.status,
                    CURRENT_DATE
                FROM stg_orders o
                WHERE o.quantity > 0
            """)

        refresh_dim_customers()
        refresh_dim_products()
        refresh_fact_orders()

    # ---- DATA QUALITY ---- #
    @task
    def dq_check_customer_dim():
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        count = hook.get_first("SELECT COUNT(*) FROM dim_customers")[0]
        if count == 0:
            raise ValueError("❌ Data Quality Failed: dim_customers is empty!")
        return count

    @task
    def dq_check_fact_orders_not_empty():
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        count = hook.get_first("SELECT COUNT(*) FROM fact_orders WHERE order_date = CURRENT_DATE")[0]
        if count == 0:
            raise ValueError("❌ Data Quality Failed: fact_orders has no valid records!")
        return count

    @task
    def dq_check_fact_orders_nulls():
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        bad = hook.get_first("""
            SELECT COUNT(*) 
            FROM fact_orders 
            WHERE customer_id IS NULL OR product_id IS NULL
        """)[0]
        if bad > 0:
            raise ValueError("❌ Data Quality Failed: fact_orders contains NULL keys!")
        return bad

    # ---- BRANCHING ---- #
    @task
    def check_order_volume(count: int):
        threshold = int(Variable.get("shopverse_min_order_threshold", 10))
        if count < threshold:
            return "warn_low_volume"
        return "normal_flow"

    warn_low_volume = EmptyOperator(task_id="warn_low_volume")
    normal_flow = EmptyOperator(task_id="normal_flow")

    # ---- FAILURE NOTIFICATION ---- #
    notify_failure = EmailOperator(
        task_id="notify_failure",
        to="alert@example.com",
        subject="ShopVerse ETL FAILED",
        html_content="The ShopVerse pipeline failed. Please investigate.",
        trigger_rule="one_failed"  # Only trigger if any upstream task fails
    )

    # ---- Task Dependencies ---- #
    [wait_customers, wait_products, wait_orders] >> staging
    staging >> warehouse

    customer_dq = dq_check_customer_dim()
    fact_nonempty = dq_check_fact_orders_not_empty()
    null_check = dq_check_fact_orders_nulls()

    warehouse >> [customer_dq, fact_nonempty, null_check]

    volume_check = check_order_volume(fact_nonempty)
    volume_check >> [warn_low_volume, normal_flow]

    # Notify only if checks fail
    [customer_dq, fact_nonempty, null_check] >> notify_failure


dag = shopverse_daily_pipeline()
