# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "246f6b0f-8f7c-4354-9496-eb05180e8a4d",
# META       "default_lakehouse_name": "LH_Retail_Gold",
# META       "default_lakehouse_workspace_id": "282bb445-cbe5-48cb-9b17-ecf418a0e949",
# META       "known_lakehouses": [
# META         {
# META           "id": "246f6b0f-8f7c-4354-9496-eb05180e8a4d"
# META         },
# META         {
# META           "id": "646322aa-7fe1-4e41-be88-b13e96f356db"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_Retail_Silver.dbo.silver_customers LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Gold Layer - Load Silver Tables
# ============================================

from pyspark.sql import functions as F

silver_customers = spark.table(
    "LH_Retail_Silver.dbo.silver_customers"
)

silver_orders = spark.table(
    "LH_Retail_Silver.dbo.silver_orders"
)

silver_orderdetails = spark.table(
    "LH_Retail_Silver.dbo.silver_orderdetails"
)

silver_products = spark.table(
    "LH_Retail_Silver.dbo.silver_products"
)

print("Silver tables loaded successfully.")

print("Customers:", silver_customers.count())
print("Orders:", silver_orders.count())
print("OrderDetails:", silver_orderdetails.count())
print("Products:", silver_products.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# GOLD LAYER - DIM CUSTOMER
# ============================================

from pyspark.sql import functions as F

dim_customer = (
    silver_customers
    .select(
        "CustomerID",
        "CustomerName",
        "Email",
        "Phone",
        "City",
        "State",
        "Country",
        "CustomerSegment"
    )
    .dropDuplicates(["CustomerID"])
)

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_customer")

print("Gold dim_customer created successfully.")
print("Rows:", dim_customer.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# GOLD LAYER - DIM PRODUCT
# ============================================

dim_product = (
    silver_products
    .select(
        "ProductID",
        "ProductName",
        "Category",
        "SubCategory",
        "UnitPrice"
    )
    .dropDuplicates(["ProductID"])
)

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_product")

print("Gold dim_product created successfully.")
print("Rows:", dim_product.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# GOLD LAYER - DIM DATE
# ============================================

from pyspark.sql import functions as F

# Get minimum and maximum order dates
date_range = silver_orders.select(
    F.min("OrderDate").alias("min_date"),
    F.max("OrderDate").alias("max_date")
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

print("Date Range:", min_date, "to", max_date)

# Generate calendar
dim_date = (
    spark.range(
        1
    )
    .select(
        F.sequence(
            F.to_date(F.lit(min_date)),
            F.to_date(F.lit(max_date)),
            F.expr("interval 1 day")
        ).alias("date_array")
    )
    .select(F.explode("date_array").alias("Date"))
    .withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("Date"))
    .withColumn("Quarter", F.quarter("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("MonthName", F.date_format("Date", "MMMM"))
    .withColumn("Day", F.dayofmonth("Date"))
    .withColumn("DayName", F.date_format("Date", "EEEE"))
)

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_date")

print("Gold dim_date created successfully.")
print("Rows:", dim_date.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# GOLD LAYER - FACT SALES
# ============================================

fact_sales = (
    silver_orderdetails.alias("od")
    .join(
        silver_orders.alias("o"),
        F.col("od.OrderID") == F.col("o.OrderID"),
        "inner"
    )
    .select(
        F.col("od.OrderDetailID"),
        F.col("od.OrderID"),
        F.col("o.CustomerID"),
        F.col("od.ProductID"),
        F.to_date(F.col("o.OrderDate")).alias("OrderDate"),
        F.col("od.Quantity"),
        F.col("od.UnitPrice"),
        F.col("od.DiscountPercent"),
        F.col("od.LineAmount")
    )
)

# Add DateKey for relationship with dim_date
fact_sales = fact_sales.withColumn(
    "DateKey",
    F.date_format("OrderDate", "yyyyMMdd").cast("int")
)

# Reorder columns
fact_sales = fact_sales.select(
    "OrderDetailID",
    "OrderID",
    "CustomerID",
    "ProductID",
    "DateKey",
    "OrderDate",
    "Quantity",
    "UnitPrice",
    "DiscountPercent",
    "LineAmount"
)

# Write Gold Fact table
fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_sales")

print("Gold fact_sales created successfully.")
print("Rows:", fact_sales.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# GOLD LAYER - FINAL VALIDATION
# ============================================

gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales"
]

for table in gold_tables:
    df = spark.table(table)
    print(f"{table}: {df.count()} rows")

print("\n=== Gold Tables ===")

spark.sql("SHOW TABLES") \
    .filter(F.col("tableName").isin(gold_tables)) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# GOLD LAYER - DATA QUALITY CHECKS
# ============================================

print("=== FACT SALES QUALITY CHECKS ===")

# Null checks
fact_sales.select(
    F.sum(F.col("OrderID").isNull().cast("int")).alias("Null_OrderID"),
    F.sum(F.col("CustomerID").isNull().cast("int")).alias("Null_CustomerID"),
    F.sum(F.col("ProductID").isNull().cast("int")).alias("Null_ProductID"),
    F.sum(F.col("OrderDate").isNull().cast("int")).alias("Null_OrderDate"),
    F.sum(F.col("Quantity").isNull().cast("int")).alias("Null_Quantity"),
    F.sum(F.col("LineAmount").isNull().cast("int")).alias("Null_LineAmount")
).show()

# Negative / invalid values
print("=== INVALID VALUE CHECKS ===")

print("Negative Quantity:",
      fact_sales.filter(F.col("Quantity") <= 0).count())

print("Negative LineAmount:",
      fact_sales.filter(F.col("LineAmount") < 0).count())

print("Duplicate OrderDetailID:",
      fact_sales.groupBy("OrderDetailID")
      .count()
      .filter(F.col("count") > 1)
      .count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Layer – Retail Analytics
# 
# The Gold layer contains business-ready dimensional and fact tables designed for analytics and Power BI reporting.
# 
# ## Gold Tables
# 
# - **dim_customer** – Customer master and segmentation information.
# - **dim_product** – Product, category, subcategory, and pricing information.
# - **dim_date** – Calendar dimension used for time-based analysis.
# - **fact_sales** – Central sales fact table containing order-level sales transactions.
# 
# ## Data Model
# 
# The Gold layer follows a **Star Schema**:
# 
# **dim_customer**  
# ↓  
# **fact_sales**  
# ↑  
# **dim_product**
# 
# **dim_date** → **fact_sales**
# 
# ## Validation
# 
# - Customers: 1,000 rows
# - Products: 100 rows
# - Date records: 119 rows
# - Sales transactions: 20,000 rows
# - Null and data-quality checks completed successfully.
# 
# ## Purpose
# 
# This Gold layer provides a clean, business-ready foundation for **Power BI semantic modeling, DAX measures, KPIs, and retail analytics**.
