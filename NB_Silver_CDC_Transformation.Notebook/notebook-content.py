# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "646322aa-7fe1-4e41-be88-b13e96f356db",
# META       "default_lakehouse_name": "LH_Retail_Silver",
# META       "default_lakehouse_workspace_id": "282bb445-cbe5-48cb-9b17-ecf418a0e949",
# META       "known_lakehouses": [
# META         {
# META           "id": "646322aa-7fe1-4e41-be88-b13e96f356db"
# META         },
# META         {
# META           "id": "b609ac20-40eb-42e6-8f5f-a26c37e44c3f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Remove incorrectly created Silver table from Bronze Lakehouse

spark.sql("""
DROP TABLE IF EXISTS LH_Retail_Bronze.dbo.silver_customers
""")

print("Incorrect silver_customers table removed from Bronze.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Spark functions loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =================================
# Silver Customers Transformation
# =================================

customers_clean = (
    bronze_customers
    .withColumn("CustomerName", F.trim(F.col("CustomerName")))
    .withColumn("Email", F.trim(F.col("Email")))
    .withColumn("Phone", F.trim(F.col("Phone")))
    .withColumn("City", F.trim(F.col("City")))
    .withColumn("State", F.trim(F.col("State")))
    .withColumn("Country", F.trim(F.col("Country")))
    .withColumn("CustomerSegment", F.trim(F.col("CustomerSegment")))
)

# Convert empty strings to NULL
text_columns = [
    "CustomerName",
    "Email",
    "Phone",
    "City",
    "State",
    "Country",
    "CustomerSegment"
]

for col_name in text_columns:
    customers_clean = customers_clean.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None)
         .otherwise(F.col(col_name))
    )

# Keep latest record for each CustomerID
window_spec = Window.partitionBy("CustomerID").orderBy(
    F.col("ModifiedDate").desc_nulls_last()
)

silver_customers = (
    customers_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Write to DEFAULT = LH_Retail_Silver
silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")

print("Silver Customers created successfully.")
print("Rows:", silver_customers.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Silver Layer - Orders Transformation
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load Orders from Bronze
bronze_orders = spark.table("LH_Retail_Bronze.dbo.Orders")

print("Bronze Orders loaded:", bronze_orders.count())

# Clean Orders
orders_clean = (
    bronze_orders
    .withColumn("OrderStatus", F.trim(F.col("OrderStatus")))
    .withColumn("PaymentMethod", F.trim(F.col("PaymentMethod")))
    .withColumn("ShippingCity", F.trim(F.col("ShippingCity")))
    .withColumn("ShippingState", F.trim(F.col("ShippingState")))
)

# Convert empty strings to NULL
text_columns = [
    "OrderStatus",
    "PaymentMethod",
    "ShippingCity",
    "ShippingState"
]

for col_name in text_columns:
    orders_clean = orders_clean.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None)
         .otherwise(F.col(col_name))
    )

# Keep latest record for each OrderID
window_spec = Window.partitionBy("OrderID").orderBy(
    F.col("ModifiedDate").desc_nulls_last()
)

silver_orders = (
    orders_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Write to Silver Lakehouse
silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

print("Silver Orders created successfully.")
print("Rows:", silver_orders.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Silver Layer - OrderDetails Transformation
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load OrderDetails from Bronze
bronze_orderdetails = spark.table(
    "LH_Retail_Bronze.dbo.OrderDetails"
)

print("Bronze OrderDetails loaded:", bronze_orderdetails.count())

# Clean and standardize data types
orderdetails_clean = (
    bronze_orderdetails
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    .withColumn("DiscountPercent", F.col("DiscountPercent").cast("double"))
    .withColumn("LineAmount", F.col("LineAmount").cast("double"))
)

# Remove invalid quantities
orderdetails_clean = orderdetails_clean.filter(
    F.col("Quantity").isNotNull() &
    (F.col("Quantity") > 0)
)

# Remove duplicate OrderDetailID records
silver_orderdetails = (
    orderdetails_clean
    .dropDuplicates(["OrderDetailID"])
)

# Write to Silver Lakehouse
silver_orderdetails.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orderdetails")

print("Silver OrderDetails created successfully.")
print("Rows:", silver_orderdetails.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Silver Layer - Products Transformation
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load Products from Bronze
bronze_products = spark.table(
    "LH_Retail_Bronze.dbo.Products"
)

print("Bronze Products loaded:", bronze_products.count())

# Clean Products
products_clean = (
    bronze_products
    .withColumn("ProductName", F.trim(F.col("ProductName")))
    .withColumn("Category", F.trim(F.col("Category")))
)

# Convert empty strings to NULL
text_columns = [
    "ProductName",
    "Category"
]

for col_name in text_columns:
    products_clean = products_clean.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None)
         .otherwise(F.col(col_name))
    )

# Standardize numeric columns
products_clean = (
    products_clean
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
)

# Remove duplicate ProductID records
silver_products = (
    products_clean
    .dropDuplicates(["ProductID"])
)

# Write to Silver Lakehouse
silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_products")

print("Silver Products created successfully.")
print("Rows:", silver_products.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Silver Layer - Final Validation
# ============================================

silver_tables = [
    "silver_customers",
    "silver_orders",
    "silver_orderdetails",
    "silver_products"
]

for table in silver_tables:
    df = spark.table(table)
    print(f"{table}: {df.count()} rows")

print("\n=== Silver Tables ===")

spark.sql("SHOW TABLES").filter(
    F.col("tableName").startswith("silver_")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Silver Layer – Data Transformation & Validation
# 
# The Silver layer contains cleaned, standardized, and deduplicated data
# transformed from the Bronze layer.
# 
# ### Silver Tables
# 
# - **silver_customers** – Cleaned customer master data
# - **silver_orders** – Cleaned and deduplicated order data
# - **silver_orderdetails** – Standardized order-line transaction data
# - **silver_products** – Cleaned product master data
# 
# ### Validation Results
# 
# | Table | Rows |
# |---|---:|
# | silver_customers | 1,000 |
# | silver_orders | 10,000 |
# | silver_orderdetails | 20,000 |
# | silver_products | 100 |
# 
# All four Silver tables were successfully created in
# **LH_Retail_Silver** and validated successfully.
# 
# The Silver layer is now ready for the **Gold layer transformation**.

# MARKDOWN ********************

