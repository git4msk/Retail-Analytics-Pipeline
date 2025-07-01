# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.projectretail.dfs.core.windows.net",
    "2HaSMMEq2WXOU5h8nm6GnNAvl0EdSmPVCoEMsGl9OkJv4HYjszizhthUMNmNN+fDTFAzcN+sotFQ+AStS99XrA=="
)

retail_bronze = spark.read.format("delta").load(
    "abfss://bronze@projectretail.dfs.core.windows.net/online_retail"
)

country_bronze = spark.read.format("delta").load(
    "abfss://bronze@projectretail.dfs.core.windows.net/country_data"
)

display(retail_bronze)
display(country_bronze)

# COMMAND ----------

retail_bronze = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("abfss://raw@projectretail.dfs.core.windows.net/online_retail.csv")
from pyspark.sql.functions import col, trim, to_date, month, initcap

retail_silver = retail_bronze.select(
    trim(col("Invoice")).alias("invoice"),
    trim(col("stock_code")).alias("stock_code"),
    trim(col("description")).alias("description"),
    col("quantity").cast("int"),
    to_date(col("invoice_date")).alias("invoice_date"),
    col("unit_price").cast("double"),
    col("total_amount").cast("double"),
    initcap(trim(col("country"))).alias("country")
).filter(
    (col("invoice").isNotNull()) &
    (col("stock_code").isNotNull()) &
    (col("invoice_date").isNotNull()) &
    (col("quantity") > 0)
)

retail_silver = retail_silver.withColumn("month_of_purchase", month("invoice_date"))
retail_silver.display()


# COMMAND ----------

retail_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
    "abfss://silver@projectretail.dfs.core.windows.net/online_retail_clean"
)

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap

# Step 2: Clean and transform
country_silver = country_bronze.select(
    trim(col("country")).alias("country"),
    col("population").cast("long"),
    trim(col("region")).alias("region"),
    trim(col("income_level")).alias("income_level"),
    col("gdp_per_capita").cast("double"),
    col("percent_internet_users").cast("double")
).dropna(subset=["country", "population", "gdp_per_capita", "percent_internet_users"])

# Step 3: Format strings (title case)
country_silver = country_silver.withColumn("country", initcap(col("country"))) \
                               .withColumn("region", initcap(col("region"))) \
                               .withColumn("income_level", initcap(col("income_level")))


country_silver.display()

# COMMAND ----------

country_silver.write.format("delta").mode("overwrite").save(
    "abfss://silver@projectretail.dfs.core.windows.net/country_data_clean"
)

# COMMAND ----------

from pyspark.sql.functions import col, lower

# Step 2: Normalize country names to lowercase for join
retail_silver = retail_silver.withColumn("country", lower(col("country")))
country_silver = country_silver.withColumn("country", lower(col("country")))

# Step 3: Perform inner join on 'country'
retail_country_joined = retail_silver.join(
    country_silver, on="country", how="inner"
)

# Step 4: Select only relevant columns (no invoice, no invoice_date, no revenue_per_capita)
final_gold = retail_country_joined.select(
    "description",
    "quantity",
    "unit_price",
    "total_amount",
    "month_of_purchase",
    "country",
    "region",
    "income_level",
    "population",
    "gdp_per_capita",
    "percent_internet_users"
)


# COMMAND ----------

final_gold.display()

# COMMAND ----------

final_gold.write.format("delta").mode("overwrite").save(
    "abfss://gold@projectretail.dfs.core.windows.net/gold/retail_population_joined"
)


# COMMAND ----------

