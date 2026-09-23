# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9e31c825-c50d-4153-92b6-153ab8c8ef80",
# META       "default_lakehouse_name": "LH_Retail_Bronze",
# META       "default_lakehouse_workspace_id": "859f1279-ca40-4f84-ac03-dcc106c8e419",
# META       "known_lakehouses": [
# META         {
# META           "id": "9e31c825-c50d-4153-92b6-153ab8c8ef80"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the row count in bronze layer
customers_bronze = spark.table("Customers")
orders_bronze = spark.table("Orders")
orderdetails_bronze = spark.table("OrderDetails")
products_bronze = spark.table("Products")

print("Customers:", customers_bronze.count())
print("Orders:", orders_bronze.count())
print("OrderDetails:", orderdetails_bronze.count())
print("Products:", products_bronze.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect Schemas
print("=== CUSTOMERS ===")
customers_bronze.printSchema()

print("=== ORDERS ===")
orders_bronze.printSchema()

print("=== ORDER DETAILS ===")
orderdetails_bronze.printSchema()

print("=== PRODUCTS ===")
products_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identify all columns and CDC fields
for name, df in [
    ("Customers", customers_bronze),
    ("Orders", orders_bronze),
    ("OrderDetails", orderdetails_bronze),
    ("Products", products_bronze)
]:
    print(f"\n=== {name} ===")
    print(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check data quality
# Check sample records and data types

for name, df in [
    ("Customers", customers_bronze),
    ("Orders", orders_bronze),
    ("OrderDetails", orderdetails_bronze),
    ("Products", products_bronze)
]:
    print(f"\n{'='*20} {name} {'='*20}")
    df.show(5, truncate=False)
    df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver Customers
# 
# - Remove duplicate CustomerID
# 
# - Trim text columns
# 
# - Standardize empty strings to NULL
# 
# - Keep the latest record based on ModifiedDate
# 
# - Write the result as silver_customers

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Clean Customers
customers_clean = (
    customers_bronze
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
    "CustomerName", "Email", "Phone",
    "City", "State", "Country", "CustomerSegment"
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

# Write Silver table
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

# MARKDOWN ********************

# ### Silver Orders
# 
# - Trim text fields
# - Convert blank values to NULL
# - Keep the latest record per OrderID
# - Preserve CreatedDate / ModifiedDate
# - Create a Delta table: silver_orders

# CELL ********************

# ==========================================
# Silver Orders Transformation
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Bronze Orders
orders_bronze = spark.table("Orders")

# Clean text columns
orders_clean = (
    orders_bronze
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
        F.when(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
            F.lit(None)
        ).otherwise(F.col(col_name))
    )

# Keep the latest record for each OrderID
window_spec = (
    Window
    .partitionBy("OrderID")
    .orderBy(F.col("ModifiedDate").desc_nulls_last())
)

silver_orders = (
    orders_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Write Silver table
(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_orders")
)

print("Silver Orders created successfully.")
print("Rows:", silver_orders.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver OrderDetails
# 
# - Trim/clean the data
# - Validate numeric values
# - Remove duplicate OrderDetailID
# - Keep the latest record if duplicates exist
# - Create silver_orderdetails

# CELL ********************

# =================================
# Silver OrderDetails Transformation
# =================================

orderdetails_clean = (
    orderdetails_bronze
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    .withColumn("DiscountPercent", F.col("DiscountPercent").cast("double"))
    .withColumn("LineAmount", F.col("LineAmount").cast("double"))
)

# Keep one record per OrderDetailID
window_spec = Window.partitionBy("OrderDetailID").orderBy(
    F.col("OrderDetailID").desc()
)

silver_orderdetails = (
    orderdetails_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Write Silver table
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

# ================================
# Silver Products Transformation
# ================================

products_clean = (
    products_bronze
    .withColumn("ProductName", F.trim(F.col("ProductName")))
    .withColumn("Category", F.trim(F.col("Category")))
    .withColumn("SubCategory", F.trim(F.col("SubCategory")))
    .withColumn("Supplier", F.trim(F.col("Supplier")))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    .withColumn("CostPrice", F.col("CostPrice").cast("double"))
    .withColumn("StockQuantity", F.col("StockQuantity").cast("int"))
)

# Convert empty strings to NULL
text_columns = [
    "ProductName",
    "Category",
    "SubCategory",
    "Supplier"
]

for col_name in text_columns:
    products_clean = products_clean.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None)
         .otherwise(F.col(col_name))
    )

# Keep latest record for each ProductID
window_spec = Window.partitionBy("ProductID").orderBy(
    F.col("ModifiedDate").desc_nulls_last()
)

silver_products = (
    products_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Write Silver table
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

# ==========================================
# Silver Layer - Final Validation
# ==========================================

silver_tables = [
    "silver_customers",
    "silver_orders",
    "silver_orderdetails",
    "silver_products"
]

for table in silver_tables:
    df = spark.table(table)
    print(f"{table}: {df.count()} rows")

print("\nSilver tables:")
spark.sql("SHOW TABLES").filter(
    F.col("tableName").startswith("silver_")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
