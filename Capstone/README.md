🚢 **WORLD EXPORTS & GLOBAL TRADE PERFORMANCE ANALYTICS PLATFORM**

**Technologies Used:-** *Azure Databricks | Apache Airflow | Spark | Power BI*

**• PROJECT OVERVIEW**

This project implements an end-to-end data engineering pipeline to analyze Global Trade Export Performance using the Medallion Architecture (Bronze → Silver → Gold) on Azure Databricks, orchestrated using Apache Airflow, and visualized in Power BI.

The pipeline ingests export datasets, applies validation and cleaning, builds curated analytical datasets, and generates insights such as:

  -> Top performing exporting countries
  
  -> Product-wise export performance
  
  -> Regional trade analytics
  
  -> Year-over-year growth trends
  
  -> Emerging market identification

**• ARCHITECTURE OVERVIEW**

This project follows Medallion Architecture:

Raw Source Data

⬇️

BRONZE  ➡️ Raw Ingestion Layer

⬇️
        
SILVER  ➡️ Cleaned & Standardized Layer

⬇️
        
GOLD    ➡️ Business & Analytics Layer

⬇️
        
POWER BI Dashboards

Workflow is scheduled and controlled using Apache Airflow, and Unity Catalog lineage tracks data flow from Bronze → Silver → Gold.

<hr>

***🧾 Dataset Description***

1️⃣ Global Export Fact Dataset

The schema of this dataset is:

Country_Name

Country_Code

Year

Month

Product_Code

Product_Name

Product_Category

Region

Export_Value_USD

Export_Units


2️⃣ Country Reference Dataset

The schema of this datset is:

Country_Code

Country_Name

Region

3️⃣ Product Reference Dataset

The schema of this dataset is:

Product_Code

Product_Name

Product_Category

The two reference datasets are used so that they act as trusted lookup that ensure the fact data is consistent, accurate, and standardized.

The datasets are initially stored in the unity catalog of databricks. It uses built-in storage "Volumes" to store the datasets. The URL path looks something like "/Volumes/capstone/default/datasets/".
<hr>

**• DATA PROCESSING**

🔷 Bronze Layer (Raw Ingestion)

Platform: Azure Databricks

Technology: PySpark + Delta Tables

✔️ Responsibilities

-> Read raw CSV files

-> Validate schema

-> Validate country and product references

-> Load into Bronze Delta Tables

Bronze tables are created in

capstone.default.bronze_fact

capstone.default.bronze_country

capstone.default.bronze_product

🔶 Silver Layer (Data Cleaning & Standardization)

Platform: Azure Databricks

Technology: PySpark Transformations

✔️ Responsibilities

-> Remove null critical fields

-> Remove negative values

-> Standardize country / product attributes using references

-> Remove duplicates

-> Cast to correct data types

The silver is created in 

capstone.default.silver_fact

Result: trusted standardized dataset

🟡 Gold Layer (Analytics & Business Layer)

Platform: Azure Databricks

Technology: Aggregations + Window Functions

✔️ Gold Tables Created

&emsp;&emsp;&emsp; Table	 &emsp;&emsp;&emsp;&emsp;&emsp;                       Purpose

gold_country_performance &emsp;&emsp;&nbsp;&ensp;   Top exporting countries with ranking

gold_product_performance &emsp;&emsp;&ensp;    Product-wise performance

gold_region_performance &emsp;&emsp;&ensp;&nbsp;&ensp;    Region-level analytics

gold_country_growth_trends&emsp;&emsp;&nbsp;  YoY growth %

gold_product_growth_trends&emsp;&emsp;  Product YoY growth

gold_region_product_matrix &emsp;&emsp;  Region vs Product export trends

gold_emerging_markets &emsp;&emsp;&emsp;&emsp;    Countries growing consistently

These datasets are analytics ready and directly consumed by Power BI.






