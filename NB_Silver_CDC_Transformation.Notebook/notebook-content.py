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
