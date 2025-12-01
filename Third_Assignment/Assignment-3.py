# Databricks notebook source
# MAGIC %md
# MAGIC 1. Ingest sample order data into a Spark DataFrame.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import to_date, col
import uuid, random, datetime

spark = SparkSession.builder.getOrCreate()

# Parameters
N_ROWS = 10000   
START = datetime.datetime(2025, 10, 1, 0, 0)
END   = datetime.datetime(2025, 11, 15, 23, 59)
COUNTRIES = ["US","IN","GB","DE","FR","BR","CA"]
STATUSES = ["CREATED","PAID","CANCELLED"]
CURRENCIES = {"US":"USD","IN":"INR","GB":"GBP","DE":"EUR","FR":"EUR","BR":"BRL","CA":"CAD"}

def rand_ts(s,e):
    return datetime.datetime.utcfromtimestamp(random.randint(int(s.timestamp()), int(e.timestamp())))

rows = []
for _ in range(N_ROWS):
    oid = str(uuid.uuid4())
    ts = rand_ts(START, END)
    cid = f"{random.randint(1,4000)}"
    country = random.choice(COUNTRIES)
    amount = round(random.uniform(1.0,500.0),2)
    currency = CURRENCIES[country]
    status = random.choices(STATUSES, weights=[0.1,0.85,0.05])[0]
    rows.append((oid, ts, cid, country, float(amount), currency, status))

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("country", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False)
])

df_orders = spark.createDataFrame(rows, schema)
display(df_orders.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Add a derived column order_date (date only from order_timestamp).
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import to_date

df_orders = df_orders.withColumn("order_date", to_date(col("order_timestamp")))
display(df_orders.select("order_id","order_timestamp","order_date").limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Write the DataFrame as a Delta table partitioned by country and order_date.
# MAGIC

# COMMAND ----------

# setting up the volume path
volume_path = "/Volumes/main/default/orders_vol/orders_delta"

(df_orders
 .write
 .format("delta")
 .mode("overwrite")
 .partitionBy("country", "order_date")
 .save(volume_path))


# COMMAND ----------

# MAGIC %md
# MAGIC 4. Verify the partition structure in the storage path.
# MAGIC
# MAGIC

# COMMAND ----------

#displays the structure in orders_delta
display(dbutils.fs.ls("/Volumes/main/default/orders_vol/orders_delta"))


# COMMAND ----------

#displays the structure in orders_delta with tha country partitioning
display(dbutils.fs.ls("/Volumes/main/default/orders_vol/orders_delta/country=IN"))


# COMMAND ----------

#displays partitioning structure with a specified condition, a country along with the order date
display(dbutils.fs.ls("/Volumes/main/default/orders_vol/orders_delta/country=IN/order_date=2025-10-10"))


# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run queries that demonstrate partition pruning (e.g., filter on a single country and/or date).
# MAGIC

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/main/default/orders_vol/orders_delta")
#the next commands will return the first record present in the table
sample = df.select("country", "order_date").limit(1).collect()[0]
sample_country = sample["country"]
sample_date = sample["order_date"]

print("Country:", sample_country)
print("Order Date:", sample_date)


# COMMAND ----------

#applying a filter condition and display all records who;s count
df_pruned = df.filter(
    (df.country == sample_country) &
    (df.order_date == sample_date)
)
display(df_pruned)
df_pruned.count()

# COMMAND ----------

#without pruned data
df_nonpruned = df.filter(df.customer_id == "cust_50")
#displays the plan
df_nonpruned.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC 6. Demonstrate Delta Lake Time Travel:
# MAGIC Write data, update some rows, then query older versions.
# MAGIC

# COMMAND ----------

df.writeTo("main.default.orders_data").createOrReplace()


# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark, "main.default.orders_data")
display(deltaTable.history())



# COMMAND ----------

#we make some new changes into the table
from pyspark.sql.functions import expr

sample_df = (
    spark.table("main.default.orders_data")
         .orderBy(expr("rand()"))
         .limit(5)
         .select("order_id")
)

sample_df.show()


# COMMAND ----------

sample_ids = [row.order_id for row in sample_df.collect()]
sample_ids
#updateing the rows
id_list_sql = ", ".join([f"'{x}'" for x in sample_ids])




# COMMAND ----------

from pyspark.sql.functions import lit

deltaTable.update(
    condition=f"order_id IN ({id_list_sql})",
    set={"status": lit("PAID")}
)


# COMMAND ----------

deltaHistory=deltaTable.history()
display(deltaHistory)


# COMMAND ----------

display(deltaHistory.where("version=='1'"))

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Demonstrate Schema Evolution:
# MAGIC Add payment_method & coupon_code to new data.
# MAGIC
# MAGIC Write to the same Delta table, allowing schema evolution.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import to_date, col
import uuid, random, datetime

# New batch size
N_NEW = 500

payment_methods = ["CARD", "UPI", "COD", "WALLET"]

new_rows = []
for _ in range(N_NEW):
    oid = str(uuid.uuid4())
    ts = rand_ts(START, END + datetime.timedelta(days=10))
    cid = f"{random.randint(1,4000)}"
    country = random.choice(COUNTRIES)
    amount = round(random.uniform(1.0,500.0),2)
    currency = CURRENCIES[country]
    status = random.choice(STATUSES)
    payment_method = random.choice(payment_methods)
    coupon_code = random.choice([None, f"CPN{random.randint(100,999)}", None])

    new_rows.append((oid, ts, cid, country, amount, currency, status, payment_method, coupon_code))

# Schema with new fields
schema_new = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("country", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("coupon_code", StringType(), True)
])

df_new = spark.createDataFrame(new_rows, schema_new)
df_new = df_new.withColumn("order_date", to_date(col("order_timestamp")))

display(df_new.limit(5))


# COMMAND ----------

(df_new.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")    # allow new columns
    .saveAsTable("main.default.orders_data")
)


# COMMAND ----------

#validating schema evolution
spark.sql("DESCRIBE TABLE main.default.orders_data").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC 8. Demonstrate Updates & Deletes using Delta:
# MAGIC
# MAGIC Mark some orders as CANCELLED.
# MAGIC
# MAGIC Delete orders below a certain amount (e.g., test data cleanup).
# MAGIC

# COMMAND ----------

deltaTable = DeltaTable.forName(spark, "main.default.orders_data")
deltaTable.update(
    condition="amount < 300 AND status != 'CANCELLED'",
    set={"status": lit("CANCELLED")}
)


# COMMAND ----------

#verify if update happend or no
spark.table("main.default.orders_data") \
     .filter("amount < 300") \
     .select("order_id", "amount", "status") \
     .show(20, truncate=False)


# COMMAND ----------

#deleting some rows
deltaTable.delete("amount < 100")


# COMMAND ----------

#verify delete
print(
    "Rows remaining with amount < 100:",
    spark.table("main.default.orders_data")
         .filter("amount < 100")
         .count()
)


# COMMAND ----------

display(deltaTable.history())


# COMMAND ----------

# MAGIC %md
# MAGIC 9. (Bonus) Optimize the table:
# MAGIC Use OPTIMIZE and optionally ZORDER on customer_id or order_date.
# MAGIC ****

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.default.orders_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.default.orders_data
# MAGIC ZORDER BY (customer_id);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.default.orders_data
# MAGIC ZORDER BY (order_date);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 10. (Bonus) Show how small file problems can occur with too many partitions and how OPTIMIZE helps.
# MAGIC

# COMMAND ----------

# location for small-file demo
volume_small_path = "/Volumes/main/default/orders_vol/orders_smallfiles"

# Generate many tiny files
for i in range(100):    
    tiny_df = df_orders.limit(50)
    (tiny_df.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .save(volume_small_path))


# COMMAND ----------

from delta.tables import DeltaTable

delta_small = DeltaTable.forPath(spark, "/Volumes/main/default/orders_vol/orders_smallfiles")


# COMMAND ----------

def count_files(path):
    total = 0
    for f in dbutils.fs.ls(path):
        if f.isDir():
            total += count_files(f.path)
        else:
            total += 1
    return total

before = count_files("/Volumes/main/default/orders_vol/orders_smallfiles")
print("Files BEFORE OPTIMIZE:", before)


# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/main/default/orders_vol/orders_smallfiles`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/main/default/orders_vol/orders_smallfiles`;
# MAGIC

# COMMAND ----------

