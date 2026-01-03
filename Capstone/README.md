🚢 **WORLD EXPORTS & GLOBAL TRADE PERFORMANCE ANALYTICS PLATFORM**

**Technologies Used:-** *Azure Databricks | Apache Airflow | Spark | Power BI*

***• PROJECT OVERVIEW***

This project implements an end-to-end data engineering pipeline to analyze Global Trade Export Performance using the Medallion Architecture (Bronze → Silver → Gold) on Azure Databricks, orchestrated using Apache Airflow, and visualized in Power BI.

The pipeline ingests export datasets, applies validation and cleaning, builds curated analytical datasets, and generates insights such as:

  -> Top performing exporting countries
  
  -> Product-wise export performance
  
  -> Regional trade analytics
  
  -> Year-over-year growth trends
  
  -> Emerging market identification

***• ARCHITECTURE OVERVIEW***

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

***• DATA PROCESSING***

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

Technology: Grouping, Filtering, and Aggregation  

✔️ Gold Tables Created

 Table	 &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&ensp;&ensp;&ensp;  Purpose

gold_country_performance &emsp;&emsp;&nbsp;&ensp;   Top exporting countries with ranking

gold_product_performance &emsp;&emsp;&ensp;    Product-wise performance

gold_region_performance &emsp;&emsp;&ensp;&nbsp;&ensp;    Region-level analytics

gold_country_growth_trends&emsp;&emsp;&nbsp;  YoY growth %

gold_product_growth_trends&emsp;&emsp;  Product YoY growth

gold_region_product_matrix &emsp;&emsp;  Region vs Product export trends

gold_emerging_markets &emsp;&emsp;&emsp;&emsp;    Countries growing consistently

<img width="356" height="680" alt="Screenshot 2026-01-03 at 6 19 04 PM" src="https://github.com/user-attachments/assets/2ab168d3-7a5b-47e6-b288-73de30d4e1b4" />

These datasets are analytics ready and directly consumed by Power BI.
<hr>

***🌀 AIRFLOW ORCHESTRATION***

Platform: Apache Airflow

Purpose: Execute pipeline in order with dependencies

*DAG Structure:*

bronze_layer_ingestion
            ↓
silver_layer_processing
            ↓
gold_layer_processing

Setup of Airflow:

✔️ Prerequisites

-> Python 3.10

-> pip

-> Virtual Environment (recommended)

1️⃣ Create and Activate Virtual Environment

`python3 -m venv airflow310`

`source airflow310/bin/activate`

2️⃣ Install Airflow & Databricks Provider

`pip install "apache-airflow==2.7.3"`
`pip install apache-airflow-providers-databricks`

3️⃣ Initialize Airflow

`airflow db init`

4️⃣ Create Airflow Admin User

```
airflow users create \
--username admin \ 
--password admin \ 
--firstname Admin \ 
--lastname User \ 
--role Admin \ 
--email admin@example.com
```

5️⃣ Start Airflow Services

On one terminal / cmd, run 

`airflow weserver`

On another terminal / cmd, run

`airflow scheduler`

Then access airflow UI on "http://localhost:8080" or on any other port that was mentioned.

<img width="1431" height="571" alt="Screenshot 2026-01-03 at 6 20 39 PM" src="https://github.com/user-attachments/assets/9de5503d-9e0f-425d-8795-0776b172002f" />

Connect Databricks to Airflow:

1️⃣ Create Job

-> Login to Azure portal

-> Open your Databricks workspace

-> Click on "Launch Workspace"

-> From the left navigation, click "Jobs & Pipelines"

-> Click "Create Job"

-> Fill out the configuartion details and choose the notebook

-> Add notebook task

`Task Name: bronze_task / silver_task / gold_task`

`Type: Notebook `

`Source: Workspace`

-> Select Cluster

-> Save and run the job to verify. Copy the JobID from the URL

https://adb-xxxxxxxx/jobs/931572949178308
This number is the JobID.

<img width="661" height="725" alt="Screenshot 2026-01-03 at 6 24 36 PM" src="https://github.com/user-attachments/assets/557f241d-c044-44e2-9532-41f4b9470bee" />


2️⃣ Create Connection on Airflow UI

Follow : Airflow UI → Admin → Connections → Add Connection

Fill the details:

-> Conn Id	- databricks_capstone

-> Conn Type	- Databricks

-> Host -	adb-XXXXXXXXXXXX.azuredatabricks.net (It is the workspace url path)

-> Extras -	{ "token": "YOUR_DATABRICKS_PAT_TOKEN" }

<img width="1440" height="855" alt="Screenshot 2026-01-03 at 6 21 13 PM" src="https://github.com/user-attachments/assets/51db879c-fea0-4304-8f81-7737b68996f0" />


3️⃣ Add DAG in Airflow

-> Place the DAG file into your Airflow DAGs folder:
`/Users/<username>/airflow/dags`

Example: cd ~/airflow/dags

nano global_trade_pipeline.py

Write the DAG code in the nano editor or can directly access the DAG file from the folder and write the code there.

4️⃣ Run the DAG code

-> Restart the scheduler and web server

-> Identify the DAG in the Airflow UI, turn it ON, and trigger DAG

DAG Execution order:

Bronze Layer  →  Silver Layer  →  Gold Layer

Monitor execution via:

-> Graph View

-> Gantt View

-> Task Logs

5️⃣ Verify the workflow orchestration

-> Trigger the DAG and look at its status to check if it is a SUCCESS / FAIL

-> Open the job in Databricks workspace and verify if the job has run or not

-> Open Catalog -> Select a table -> Lineage. You can verify if the table has been updated or not

✔️ Benefits

-> Full automation

-> Dependency control

-> Logs & monitoring

-> Retries & scheduling capability
<hr>

***📊 Power BI Dashboard***

The final Gold tables are visualized using Power BI.

We connect Power BI to Databricks to read analytical tables directly from Unity Catalog.

✔️ Prerequisites

-> Power BI Desktop installed

-> Access to Azure Databricks Workspace

-> Unity Catalog / Delta tables created

-> Personal Access Token (PAT)

How to generate PAT in Databricks??

User Settings → Developer → Access Tokens → Generate Token


🔗 Connect Power BI to Databricks Cluster

-> Open Power BI → Get Data

-> Search Azure Databricks

-> Add the PAT in the following steps and click Connect

-> Select the tables that you would like to load. Click "Load Data"

✔️ Key Dashboards

Country Export Leaderboard

Product Performance Trends

Region Export Comparison

Growth & Decline Analysis

Emerging Markets Report

✔️ Insights Delivered

Which countries dominate exports?

Which products drive maximum revenue?

Which regions perform best?

Who is growing consistently?

<hr>

***🧠 Data Model — Star Schema***

This project follows a Dimensional Model (Star Schema)

✔️ Fact Table

-> silver_fact

✔️ Dimension Tables

-> Country Dimension

-> Product Dimension

This improves:

-> Query performance

-> BI friendliness

-> Simplicity
<hr>

🕒 Scheduling Strategy

Currently: Manually triggered via Airflow

Supports:

 -> Daily

-> Weekly

-> Batch / Incremental

-> Event-triggered execution

The scheduling strategy can be defined in the DAG code. 

```
with DAG(
  dag_id="global_trade_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Bronze -> Silver -> Gold Databricks Pipeline",
    tags=["capstone", "databricks"]
)
```
The schedule_interval can be changed to @daily, @weekly for scheduled execution. None represents manual triggering.
<hr>

