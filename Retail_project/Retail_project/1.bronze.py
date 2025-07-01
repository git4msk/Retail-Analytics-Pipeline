# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.projectretail.dfs.core.windows.net",
  "2HaSMMEq2WXOU5h8nm6GnNAvl0EdSmPVCoEMsGl9OkJv4HYjszizhthUMNmNN+fDTFAzcN+sotFQ+AStS99XrA=="
)


# COMMAND ----------


retail_df = spark.read.option("header", True).option("inferSchema", True).csv(
  "abfss://raw@projectretail.dfs.core.windows.net/online_retail.csv"
)


country_df = spark.read.option("header", True).option("inferSchema", True).csv(
  "abfss://raw@projectretail.dfs.core.windows.net/country_data.csv"
)


# COMMAND ----------


retail_df.write.format("delta").mode("overwrite").save(
  "abfss://bronze@projectretail.dfs.core.windows.net/online_retail"
)

country_df.write.format("delta").mode("overwrite").save(
  "abfss://bronze@projectretail.dfs.core.windows.net/country_data"
)


# COMMAND ----------

