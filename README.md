# Automated AWS Airflow Data Lake

This project automates an end-to-end ETL data pipeline using **Apache Airflow**, **Apache Spark (on EMR)**, and **AWS services** such as **S3** and **Elasticsearch**.

## 🚀 Project Overview
- **Data Generator** scripts produce synthetic datasets (with and without primary keys).
- **ETL Script** processes and stores data in an **S3 Data Lake**.
- **Elasticsearch Integration** enables real-time data indexing and querying.
- **Airflow DAG** orchestrates the workflow using **Boto3 scripts**:
  1. Start EMR cluster  
  2. Run ETL script (from Git or S3)  
  3. Terminate EMR cluster  

## 🧩 Tech Stack
- **Apache Airflow**
- **Apache Spark (EMR)**
- **AWS S3**
- **Elasticsearch**
- **Python (PySpark,Boto3)**

## 🏗️ Architecture
```plaintext
Airflow DAG
    ├── Start EMR (boto3)
    ├── Run ETL (Spark + S3 + ES)
    └── Terminate EMR (boto3)
