# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Import required libraries
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/2_D_data_processing/utility

# COMMAND ----------

print(bronze_schema , silver_schema , gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog" , "fmcg" , "catalog")
dbutils.widgets.text("data_sources" , "customers" , "data_sources")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_sources = dbutils.widgets.get("data_sources")
print(catalog , data_sources)

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_sources = dbutils.widgets.get("data_sources")
base_path = f"{catalog}.{data_sources}"
print(base_path)


# COMMAND ----------

# DBTITLE 1,Cell 7
catalog = dbutils.widgets.get("catalog")
data_sources = dbutils.widgets.get("data_sources")
base_path = f"s3://sportsbar-db-15-03-2025/{data_sources}/*.csv"
print(base_path)

# COMMAND ----------

# DBTITLE 1,Cell 8
df = (
    spark.read.format("csv")
    .option("header" , True)
    .option("inferSchema" , True)
    .load(base_path)
    .withColumn("read_timestamp", f.current_timestamp())
    .select("*")
)
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

bronze_schema = "bronze"
                

# COMMAND ----------

df.write\
.format("delta")\
.option("delta.enableChangeDataFeed" , "true")\
.mode("overwrite")\
.saveAsTable(f"{catalog}.{bronze_schema}.{data_sources}")

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_sources}")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 14
df_duplicates = df.groupBy("customer_id").count().filter("count > 1")
display(df_duplicates)

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_sources = dbutils.widgets.get("data_sources")

bronze_schema = "bronze"

df_silver = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_sources}")

# COMMAND ----------

display(
    df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name")))
)

# COMMAND ----------

# DBTITLE 1,Cell 18
df_silver = df_silver.withColumn(
    "customer_name", f.trim(f.col("customer_name"))
)

# COMMAND ----------

display(
    df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name")))
)

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

city_mapping = {
    "Bengaluruu" : "Bengaluru",
    "Bangalore" : "Bengaluru",
    "Hyderabaddd": "Hyderabad",
    "Hyderbad" : "Hyderabad",
    "Newdelhi" : "New delhi",
    "NewDelhee" : "New delhi",
    "NewDheli" : "New delhi"
}
allowed = ["Bengaluru", "Hyderabad", "New delhi"]

df_silver = (
    df_silver
    .replace(city_mapping , subset = ["city"])
    .withColumn(
        "city" ,
         f.when(f.col("city").isNull() , None)
          .when(f.col("city").isin(allowed) , f.col("city"))
          .otherwise(None)
    )
)
df_silver.select("city").distinct().show()

# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    f.initcap(f.trim(f.col("customer_name")))
)
df_silver.select("customer_name").distinct().show()


# COMMAND ----------

df_silver.filter(f.col("city").isNull()).show(truncate=False)


# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(f.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------


# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        f.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(f.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", f.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        f.concat_ws("-", "customer_name", f.coalesce(f.col("city"), f.lit("Unknown")))
    )
    
    # Static attributes aligned with parent data model
    .withColumn("market", f.lit("India"))
    .withColumn("platform", f.lit("Sports Bar"))
    .withColumn("channel", f.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 31
catalog = dbutils.widgets.get("catalog")
data_sources = dbutils.widgets.get("data_sources")
silver_schema = "silver"

print(catalog, data_sources, silver_schema)

# COMMAND ----------

df_silver.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_sources}")

# COMMAND ----------

df_check = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_sources}")
display(df_check.limit(10))

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_sources}")

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_sources};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_sources}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    f.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

df_child_customers.groupBy("customer_code").count().filter("count > 1").show()

# COMMAND ----------

df_child_customers = df_child_customers.dropDuplicates(["customer_code"])

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()