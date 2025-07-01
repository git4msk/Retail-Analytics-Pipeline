##  Retail Marketing Analytics Project

This **end-to-end retail marketing analytics project** is developed using **Azure Databricks**, **Azure Data Lake Storage Gen2**, and **Delta Lake**. It integrates retail transaction with country data to deliver marketing and advertising insights by region.

---

##  Project Overview

The project focuses on enhancing marketing strategies through data-driven insights. By combining retail sales data with demographic and economic indicators such as population, GDP per capita, and internet usage, businesses can:

* Identify top-performing products by country and region
* Tailor advertising efforts based on socioeconomic patterns
* Optimize product promotions and regional marketing budgets

This project simulates a real-world scenario where regional marketing teams depend on reliable, enriched data to drive campaign decisions.

---

##  Project Requirements

###  Data Ingestion:

* Ingest retail and country-level CSV data into the data lake
* Apply appropriate schema and data types
* Store raw data in the **Bronze layer** using Delta format

###  Data Transformation:

* Clean and enrich retail and country data (e.g., trim strings, cast data types, remove invalid rows)
* Add computed columns such as `total_amount` and `month_of_purchase`
* Join datasets on `country`
* Store structured data in **Silver** and **Gold layers**

###  Analysis:

* Use **Spark SQL** to query Gold layer and extract marketing insights
* Examples include: revenue by region, top products by country, monthly sales trends, and revenue per capita

###  Business Intelligence (Optional):

* Power BI or Databricks Dashboards for visualizing KPIs like:

  * Top-performing countries/regions
  * Revenue vs internet usage
  * Product demand by region

---

##  Tools & Technologies Used

* **Azure Databricks**
* **Azure Data Lake Storage Gen2 (ADLS)**
* **Delta Lake**
* **PySpark & Spark SQL**
* **Databricks Workflows / Jobs**
* **Power BI / Dashboards (optional)**
* **GitHub** (for version control and collaboration)

---

##  Solution Architecture

This solution follows the **Modern Data Architecture** pattern using **multihop design**:

###  Bronze Layer:

* Stores raw, unprocessed CSV data
* No schema enforcement, just ingestion
* Used for traceability and rollback if needed

###  Silver Layer:

* Applies cleaning, validation, and enrichment
* Ensures schema consistency, removes nulls and incorrect rows
* Adds computed fields like `total_amount` and `month_of_purchase`

###  Gold Layer:

* Final analytics dataset ready for querying
* Joins `retail_silver` with `country_silver` on `country`
* Used for business reporting and marketing analysis

---

##  Analysis Using SQL

All insights are generated using **Spark SQL** in the Gold Layer.

Key queries include:

* Revenue by Region
* Top Products by Country
* Monthly Sales Trends
* Revenue vs Population or GDP
* Internet Usage vs Sales Performance

SQL examples are provided in the `queries/` folder.

---

##  Project Outcome

The output of this project is a **fully joined and enriched Gold dataset** stored in a Delta table. This data enables:

* Cross-regional sales performance comparison
* Strategic advertising based on GDP and internet reach
* Top-selling product identification for targeted promotion

Optionally, this Gold layer can feed into **Power BI dashboards** or **Databricks SQL Dashboards** for visual representation of KPIs.


---

##  Setup Instructions

1. Upload `online_retail.csv` and `country_data.csv` to the ADLS `raw` container.
2. Run the **Bronze notebook** to ingest data into Delta format.
3. Run the **Silver notebooks** to clean and transform data.
4. Run the **Gold notebook** to join retail and country data.
5. Use SQL notebooks to analyze the final dataset.
6. Optional: create dashboards using Databricks visual tools or Power BI.

---
