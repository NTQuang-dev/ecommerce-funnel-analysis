# E-Commerce Funnel Analysis Pipeline

## 📌 Project Overview
An end-to-end Data Engineering project that extracts raw user behavior data (10M+ events), transforms it into a structured relational schema, and loads it into a high-performance analytical database to identify conversion drop-off points.

**Goal:** To analyze the "View -> Cart -> Purchase" user journey and identify bottlenecks in the sales funnel.

## Stack
* **Language:** Python 3.10.11 (Pandas, SQLAlchemy, PyMySQL)
* **Database:** MySQL 8.0.44 (Indexed for performance)
* **Visualization:** Power BI (DAX Measures, Funnel Visualization)
* **Tools:** VS Code, Git, MySQL Workbench

## ⚙️ Key Engineering Challenges
1.  **Handling Big Data:** The raw datasets exceeded local RAM (5GB+). Implemented **stream processing (chunking)** in Python to process data in 100k-row batches.
2.  **Database Optimization:** Designed a normalized schema with **B-Tree Indices** on `user_session` to reduce query lookup time by ~90%.
3.  **Idempotent Pipeline:** Built the ETL loader to safely handle re-runs using `if_exists='append'` and transaction safety.

## 📊 The Data Pipeline
1.  **Extract:** Python scripts ingest raw CSV logs from Kaggle.
2.  **Transform:**
    * Filtered for valid funnel events (`view`, `cart`, `purchase`).
    * Downcasted numeric types to optimize memory usage.
    * Removed "orphan" events with null session IDs.
3.  **Load:** Batch-inserted cleaned data into local MySQL instance using SQLAlchemy engine.

## 📈 Results
* **Conversion Rate:** 
* **Major Drop-off:** 

## 💻 How to Run This Project
1.  Clone the repo:
    ```bash
    git clone [https://github.com/NTQuang-dev/ecommerce-funnel-analysis]
    ```
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Run the ETL pipeline:
    ```bash
    python src/cleaning.py
    python src/load_to_mysql.py
    ```