🚢 **World Exports & Global Trade Performance Analytics Platform**

**Technologies Used:-** *Azure Databricks | Apache Airflow | Delta Lake | Spark | Power BI*

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





