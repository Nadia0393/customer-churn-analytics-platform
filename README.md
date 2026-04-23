# customer-churn-analytics-platform
End-to-end Customer Churn Analytics Platform built using Databricks and Medallion Architecture (Bronze, Silver, Gold) with PySpark, Delta Lake, and Power BI for scalable data processing, feature engineering, and KPI-driven insights.

📊 Customer Churn Analytics Platform (Databricks + Medallion Architecture)
📌 Project Overview

This project is an end-to-end Customer Churn Analytics Platform built using Databricks, PySpark, and Delta Lake.
It implements a Medallion Architecture (Bronze, Silver, Gold) to process raw telecom customer data into analytics-ready datasets for churn analysis and business insights.

The solution enables identification of churn patterns, customer segmentation, and KPI-based reporting to support data-driven decision-making.

🏗️ Architecture

The pipeline follows a Medallion Architecture:

Bronze Layer → Raw data ingestion (CSV / source system data)
Silver Layer → Data cleaning, type casting, feature engineering
Gold Layer → Aggregated KPI datasets for analytics & reporting

Data flows from raw ingestion → transformation → business-ready insights.

⚙️ Tech Stack
Databricks
Apache Spark
PySpark
Delta Lake
SQL
Power BI
Python
🔄 Pipeline Workflow
1. Data Ingestion (Bronze Layer)
Loaded raw telecom customer dataset
Stored data in Delta format for processing
2. Data Cleaning & Feature Engineering (Silver Layer)
Handled missing values and data inconsistencies
Converted data types (e.g., TotalCharges)
Created features:
Tenure groups
Monthly spend categories
Customer value score
High-risk churn flags
3. KPI Aggregation (Gold Layer)
Computed business KPIs:
Total customers
Churned customers
Churn rate
Average monthly charges
Customer segmentation metrics
📊 Key Business Metrics
Customer churn rate by segment
High-risk customer identification
Revenue contribution by customer group
Customer lifetime value proxy scoring
🎯 Key Features
End-to-end ETL pipeline using Medallion Architecture
Scalable data processing using PySpark
Incremental data transformation using Delta Lake
Business-ready KPI generation
Analytics integration with Power BI
📈 Business Impact

This project enables:

Identification of high-risk churn customers
Improved customer segmentation strategies
Data-driven retention decision-making
Scalable analytics for large datasets
🧠 What I Learned
Designing scalable data pipelines using Databricks
Building Medallion Architecture from scratch
Feature engineering for customer analytics
Optimizing Spark-based transformations
KPI design for business reporting
📷 Architecture Diagram

(Add your architecture image here)
Example flow:

Raw Data → Bronze → Silver → Gold → Power BI
🚀 Future Enhancements
Add real-time streaming ingestion using Kafka
Build ML model for churn prediction
Deploy dashboard with live refresh
Implement CI/CD for data pipelines
👤 Author

Nadhiya Ganesan
Data Engineer | Databricks | Spark | Azure | DP-700 Certified
