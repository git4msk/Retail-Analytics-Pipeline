-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.key.projectretail.dfs.core.windows.net",
-- MAGIC   "2HaSMMEq2WXOU5h8nm6GnNAvl0EdSmPVCoEMsGl9OkJv4HYjszizhthUMNmNN+fDTFAzcN+sotFQ+AStS99XrA=="
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
-- MAGIC
-- MAGIC spark.sql("DROP TABLE IF EXISTS gold.retail_population_joined")
-- MAGIC
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE gold.retail_population_joined
-- MAGIC USING DELTA
-- MAGIC LOCATION 'abfss://gold@projectretail.dfs.core.windows.net/gold/retail_population_joined'
-- MAGIC """)

-- COMMAND ----------

SELECT * FROM gold.retail_population_joined LIMIT 10


-- COMMAND ----------

SELECT country, ROUND(SUM(total_amount), 2) AS total_revenue
FROM gold.retail_population_joined
GROUP BY country
ORDER BY total_revenue DESC


-- COMMAND ----------

SELECT region, ROUND(SUM(total_amount), 2) AS total_revenue
FROM gold.retail_population_joined
GROUP BY region
ORDER BY total_revenue DESC


-- COMMAND ----------

SELECT description, ROUND(SUM(total_amount), 2) AS revenue
FROM gold.retail_population_joined
GROUP BY description
ORDER BY revenue DESC
LIMIT 10


-- COMMAND ----------

SELECT month_of_purchase, ROUND(SUM(total_amount), 2) AS monthly_revenue
FROM gold.retail_population_joined
GROUP BY month_of_purchase
ORDER BY month_of_purchase


-- COMMAND ----------

SELECT income_level, ROUND(SUM(total_amount), 2) AS total_revenue
FROM gold.retail_population_joined
GROUP BY income_level
ORDER BY total_revenue DESC


-- COMMAND ----------

SELECT country, percent_internet_users
FROM gold.retail_population_joined
GROUP BY country, percent_internet_users
ORDER BY percent_internet_users DESC
LIMIT 10


-- COMMAND ----------

SELECT country, 
       SUM(total_amount) AS total_revenue, 
       MAX(population) AS population,
       ROUND(SUM(total_amount)/MAX(population), 6) AS revenue_per_person
FROM gold.retail_population_joined
GROUP BY country
ORDER BY revenue_per_person DESC
LIMIT 10


-- COMMAND ----------

SELECT country,
       ROUND(AVG(total_amount), 2) AS avg_order_value
FROM gold.retail_population_joined
GROUP BY country
ORDER BY avg_order_value DESC


-- COMMAND ----------

SELECT region, SUM(quantity) AS total_units_sold
FROM gold.retail_population_joined
GROUP BY region
ORDER BY total_units_sold DESC


-- COMMAND ----------

SELECT
  CASE
    WHEN gdp_per_capita < 5000 THEN 'Low GDP'
    WHEN gdp_per_capita BETWEEN 5000 AND 20000 THEN 'Mid GDP'
    ELSE 'High GDP'
  END AS gdp_group,
  ROUND(SUM(total_amount), 2) AS total_revenue
FROM gold.retail_population_joined
GROUP BY gdp_group
ORDER BY total_revenue DESC


-- COMMAND ----------

SELECT 
    country,
    description AS product,
    ROUND(SUM(total_amount), 2) AS revenue
FROM gold.retail_population_joined
GROUP BY country, description
ORDER BY country, revenue DESC


-- COMMAND ----------

SELECT 
    region,
    description AS product,
    ROUND(SUM(total_amount), 2) AS revenue
FROM gold.retail_population_joined
GROUP BY region, description
ORDER BY region, revenue DESC


-- COMMAND ----------

