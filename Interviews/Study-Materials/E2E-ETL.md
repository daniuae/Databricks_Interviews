# End-to-End ETL Pipeline in Databricks (Bronze → Silver → Gold)

## Overview

This document demonstrates a complete **End-to-End ETL (Extract, Transform, Load)** pipeline in Databricks using **PySpark** and **Delta Lake**. The project follows the **Medallion Architecture**, which organizes data into three layers:

* **Bronze Layer** – Raw data ingestion
* **Silver Layer** – Data cleansing and transformation
* **Gold Layer** – Business-ready aggregated data

---

# Architecture

```text
                    Source CSV
                        │
                        ▼
              DBFS / S3 / ADLS / GCS
                        │
                        ▼
                  Bronze Layer
              (Raw Delta Table)
                        │
                        ▼
                  Silver Layer
        (Cleaned & Validated Data)
                        │
                        ▼
                   Gold Layer
       (Business Aggregations)
                        │
                        ▼
          Power BI / Tableau / ML Models
```

---

# Project Structure

```text
Databricks_ETL_Project/

│
├── 01_Create_Database
├── 02_Bronze_Load
├── 03_Silver_Transformation
├── 04_Gold_Transformation
├── 05_Data_Validation
├── 06_Performance_Optimization
└── data/
      ├── customer.csv
      └── new_customer.csv
```

---

# Sample Dataset

**customer.csv**

```csv
customer_id,name,city,age,salary
101,John,London,29,6000
102,David,Paris,35,7200
103,Raj,Bangalore,31,8000
104,,Delhi,22,4500
105,Chris,London,,9000
```

Upload the file into:

```text
dbfs:/FileStore/customer/customer.csv
```

---

# Notebook 1 – Create Database

```python
# Create Database

spark.sql("""
CREATE DATABASE IF NOT EXISTS retail
""")

spark.sql("USE retail")
```

---

# Notebook 2 – Bronze Layer (Raw Data)

## Read CSV

```python
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/FileStore/customer/customer.csv")
)
```

## Display Data

```python
display(df)
```

## Write to Bronze Delta Table

```python
(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bronze_customer")
)
```

## Verify Data

```python
spark.sql("""
SELECT *
FROM bronze_customer
""").show()
```

---

# Notebook 3 – Silver Layer (Data Cleansing)

## Import Required Functions

```python
from pyspark.sql.functions import *
```

## Read Bronze Table

```python
df = spark.table("bronze_customer")
```

---

## Remove Records with Null Names

```python
df = df.filter(col("name").isNotNull())
```

---

## Replace Missing Age with 0

```python
df = df.fillna({
    "age": 0
})
```

---

## Convert City to Uppercase

```python
df = df.withColumn(
    "city",
    upper(col("city"))
)
```

---

## Add Load Timestamp

```python
df = df.withColumn(
    "load_date",
    current_timestamp()
)
```

---

## Create Age Group

```python
df = df.withColumn(
    "age_group",
    when(col("age") < 25, "Young")
    .when(col("age") < 40, "Adult")
    .otherwise("Senior")
)
```

---

## Remove Duplicate Customers

```python
df = df.dropDuplicates([
    "customer_id"
])
```

---

## Save Silver Table

```python
(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("silver_customer")
)
```

---

## Verify

```python
display(
    spark.table("silver_customer")
)
```

---

# Notebook 4 – Gold Layer (Business Aggregation)

## Read Silver Table

```python
df = spark.table("silver_customer")
```

---

## Business Aggregation

```python
gold_df = (

    df.groupBy("city")

      .agg(

          count("*").alias("customer_count"),

          avg("salary").alias("average_salary"),

          max("salary").alias("highest_salary"),

          min("salary").alias("lowest_salary")

      )

)
```

---

## Write Gold Table

```python
(
    gold_df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable("gold_customer_summary")
)
```

---

## Display Results

```python
display(
    spark.table("gold_customer_summary")
)
```

---

# Notebook 5 – Data Validation

## Total Record Count

```python
spark.sql("""

SELECT COUNT(*) AS total_records

FROM silver_customer

""").show()
```

---

## Duplicate Check

```python
spark.sql("""

SELECT
    customer_id,
    COUNT(*) AS duplicate_count

FROM silver_customer

GROUP BY customer_id

HAVING COUNT(*) > 1

""").show()
```

---

## Null Check

```python
spark.sql("""

SELECT *

FROM silver_customer

WHERE name IS NULL

""").show()
```

---

# Notebook 6 – Performance Optimization

## OPTIMIZE

```sql
OPTIMIZE silver_customer;
```

---

## ZORDER

```sql
OPTIMIZE silver_customer
ZORDER BY (customer_id);
```

---

## VACUUM

```sql
VACUUM silver_customer RETAIN 168 HOURS;
```

---

# Incremental Load (MERGE)

## New Dataset

**new_customer.csv**

```csv
customer_id,name,city,age,salary
103,Raj,Bangalore,32,9000
106,Ram,Mumbai,25,6500
```

---

## Read Incremental Data

```python
incremental_df = (

    spark.read

         .option("header", "true")

         .option("inferSchema", "true")

         .csv("/FileStore/customer/new_customer.csv")

)
```

---

## Merge Using Delta Lake

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(
    spark,
    "silver_customer"
)

(
    delta_table.alias("target")

    .merge(

        incremental_df.alias("source"),

        "target.customer_id = source.customer_id"

    )

    .whenMatchedUpdateAll()

    .whenNotMatchedInsertAll()

    .execute()
)
```

---

# Complete ETL Workflow

```text
CSV Files

     │

     ▼

Read CSV

     │

     ▼

Bronze Delta Table

     │

     ▼

Cleaning

     │

     ▼

Validation

     │

     ▼

Silver Delta Table

     │

     ▼

Business Transformations

     │

     ▼

Gold Delta Table

     │

     ▼

Dashboards / Reporting / ML
```

---

# Production Architecture

```text
Source Systems
      │
      ▼
Auto Loader / Kafka / APIs
      │
      ▼
Bronze (Raw Delta)
      │
      ▼
Silver (Cleaned Delta)
      │
      ▼
Gold (Aggregated Delta)
      │
      ▼
Unity Catalog
      │
      ▼
Power BI / Tableau / ML Models
```

---

# Technologies Used

| Layer            | Technology                |
| ---------------- | ------------------------- |
| Source           | CSV, JSON, Parquet, Kafka |
| Storage          | Delta Lake                |
| Processing       | PySpark                   |
| Workflow         | Databricks Workflows      |
| Metadata         | Unity Catalog             |
| Scheduling       | Databricks Jobs           |
| Incremental Load | MERGE                     |
| Streaming        | Auto Loader               |
| Reporting        | Power BI, Tableau         |

---

# Interview Questions

### Q1. Why do we use the Medallion Architecture?

* Separates raw, cleaned, and business data.
* Improves maintainability.
* Simplifies debugging.
* Supports incremental processing.

---

### Q2. Why Delta Lake?

* ACID Transactions
* Schema Enforcement
* Schema Evolution
* Time Travel
* MERGE Support
* Fast Reads
* Data Versioning

---

### Q3. What happens in each layer?

### Bronze

* Raw ingestion
* No transformations
* Historical storage

### Silver

* Cleansing
* Validation
* Deduplication
* Standardization

### Gold

* Aggregations
* KPIs
* Business metrics
* Reporting

---

### Q4. Why use MERGE?

MERGE performs an UPSERT operation.

* Update existing records
* Insert new records
* Avoid duplicate data

---

### Q5. Performance Optimization Techniques

* Partitioning
* OPTIMIZE
* ZORDER
* VACUUM
* Broadcast Joins
* Adaptive Query Execution (AQE)
* Caching
* Data Skipping
* Predicate Pushdown

---

# Conclusion

This project demonstrates a complete production-style ETL pipeline using Databricks and Delta Lake. It includes:

* Bronze, Silver, and Gold architecture
* Batch ingestion
* Data cleansing and validation
* Business aggregations
* Incremental processing with MERGE
* Performance optimization
* Data quality checks
* Interview-focused best practices

This is a solid foundation for enterprise-grade Databricks ETL development and can be extended with Auto Loader, Structured Streaming, Unity Catalog, and Databricks Workflows for fully automated production pipelines.
