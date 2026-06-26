# End-to-End Databricks Structured Streaming Project

## Real-Time E-Commerce Order Processing

---

# Project Overview

In this project, we will build a complete real-time data pipeline using:

- Databricks
- Apache Spark Structured Streaming
- Delta Lake
- Medallion Architecture
- Auto Loader (Optional Enhancement)

The project demonstrates how streaming data flows from:

```
Incoming Files
      │
      ▼
 Bronze Layer
      │
      ▼
 Silver Layer
      │
      ▼
 Gold Layer
      │
      ▼
 Dashboard / SQL Analytics
```

---

# Business Problem

An e-commerce company continuously receives customer orders.

Each order contains:

- Order ID
- Customer ID
- Product
- Quantity
- Price
- Order Time

The business wants to

- Process incoming orders in real time
- Remove bad records
- Calculate total sales
- Generate live dashboards
- Store data efficiently

---

# Solution Architecture

```
                  JSON Files

                       │

                       ▼

           Spark Structured Streaming

                       │

                       ▼

              Bronze Delta Table

               (Raw Streaming Data)

                       │

                       ▼

              Silver Delta Table

         (Validated + Cleaned Data)

                       │

                       ▼

               Gold Delta Table

        (Business Aggregated Data)

                       │

                       ▼

          Power BI / Tableau Dashboard
```

---

# Folder Structure

```
dbfs:/

orders_project/

    incoming_orders/

    bronze/

    silver/

    gold/

    checkpoint/

        bronze/

        silver/

        gold/
```

---

# Step 1 Create Input Files

Create folder

```
dbfs:/orders_project/incoming_orders/
```

Create file

orders1.json

```json
{"order_id":1,"customer_id":101,"product":"Laptop","quantity":1,"price":70000,"order_time":"2025-01-01T10:00:00"}

{"order_id":2,"customer_id":102,"product":"Mouse","quantity":2,"price":800,"order_time":"2025-01-01T10:00:05"}

{"order_id":3,"customer_id":103,"product":"Keyboard","quantity":1,"price":1500,"order_time":"2025-01-01T10:00:08"}
```

Later add

orders2.json

```json
{"order_id":4,"customer_id":104,"product":"Monitor","quantity":1,"price":12000,"order_time":"2025-01-01T10:01:05"}

{"order_id":5,"customer_id":105,"product":"Laptop","quantity":1,"price":72000,"order_time":"2025-01-01T10:01:20"}
```

Spark automatically detects new files.

---

# Step 2 Import Libraries

```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
```

---

# Step 3 Define Schema

```python
orderSchema = StructType([

    StructField("order_id", IntegerType()),

    StructField("customer_id", IntegerType()),

    StructField("product", StringType()),

    StructField("quantity", IntegerType()),

    StructField("price", DoubleType()),

    StructField("order_time", TimestampType())

])
```

---

# Step 4 Read Streaming Data

```python
bronzeDF = (

spark.readStream

.schema(orderSchema)

.json("/orders_project/incoming_orders")

)
```

Verify

```python
bronzeDF.isStreaming
```

Output

```
True
```

---

# Step 5 Write Bronze Layer

```python
bronzeQuery = (

bronzeDF.writeStream

.format("delta")

.option(
"checkpointLocation",
"/orders_project/checkpoint/bronze"
)

.outputMode("append")

.start("/orders_project/bronze")

)
```

Bronze stores raw incoming data.

---

# Bronze Layer Example

| order_id | customer_id | product | quantity | price |
|----------|-------------|----------|----------|--------|
|1|101|Laptop|1|70000|
|2|102|Mouse|2|800|

No transformations happen here.

---

# Step 6 Read Bronze Stream

```python
bronzeStream = (

spark.readStream

.format("delta")

.load("/orders_project/bronze")

)
```

---

# Step 7 Transform into Silver

Business Rules

- Remove invalid quantity
- Remove invalid price
- Remove duplicates
- Calculate total amount

```python
silverDF = (

bronzeStream

.filter(col("quantity") > 0)

.filter(col("price") > 0)

.dropDuplicates(["order_id"])

.withColumn(

"total_amount",

col("quantity") * col("price")

)

)
```

---

# Silver Example

Before

|Product|Qty|Price|
|-------|----|------|
|Laptop|1|70000|
|Mouse|0|800|

After

|Product|Qty|Price|Total|
|-------|----|------|------|
|Laptop|1|70000|70000|

---

# Step 8 Write Silver Layer

```python
silverQuery = (

silverDF.writeStream

.format("delta")

.option(
"checkpointLocation",
"/orders_project/checkpoint/silver"
)

.outputMode("append")

.start("/orders_project/silver")

)
```

---

# Step 9 Read Silver

```python
silverStream = (

spark.readStream

.format("delta")

.load("/orders_project/silver")

)
```

---

# Step 10 Gold Aggregation

Calculate

- Total Sales
- Total Orders

Window Duration

1 Minute

```python
goldDF = (

silverStream

.groupBy(

window(col("order_time"),"1 minute"),

col("product")

)

.agg(

sum("total_amount").alias("sales"),

count("*").alias("orders")

)

)
```

---

# Step 11 Write Gold Layer

```python
goldQuery = (

goldDF.writeStream

.format("delta")

.option(
"checkpointLocation",
"/orders_project/checkpoint/gold"
)

.outputMode("complete")

.start("/orders_project/gold")

)
```

---

# Gold Output

|Product|Sales|Orders|
|--------|------|------|
|Laptop|142000|2|
|Mouse|1600|2|
|Keyboard|1500|1|
|Monitor|12000|1|

---

# Step 12 Create SQL Table

```sql
CREATE TABLE gold_orders

USING DELTA

LOCATION '/orders_project/gold';
```

---

# Query Using SQL

```sql
SELECT *

FROM gold_orders;
```

---

# Step 13 Dashboard

Create Dashboard

Chart

```
X Axis

Product

Y Axis

Sales
```

Every new file automatically refreshes the dashboard.

---

# Streaming Lifecycle

```
JSON File Arrives

        │

        ▼

Spark Detects File

        │

        ▼

Micro Batch Created

        │

        ▼

Read Stream

        │

        ▼

Bronze

        │

        ▼

Silver

        │

        ▼

Gold

        │

        ▼

Dashboard Updated
```

---

# Check Active Streams

```python
spark.streams.active
```

---

# Monitor Streaming Progress

```python
bronzeQuery.status
```

```python
bronzeQuery.lastProgress
```

---

# Stop Streams

```python
bronzeQuery.stop()

silverQuery.stop()

goldQuery.stop()
```

---

# Fault Tolerance

Checkpoint stores

- Offsets
- Metadata
- Batch IDs
- Commit Logs

If cluster crashes

```
Restart Cluster

↓

Read Checkpoint

↓

Continue from Last Batch

↓

No Duplicate Processing
```

---

# Why Delta Lake?

Advantages

- ACID Transactions
- Schema Enforcement
- Time Travel
- Faster Reads
- Data Versioning
- Reliability

---

# Medallion Architecture

## Bronze

- Raw data
- No validation
- Historical copy

---

## Silver

- Clean data
- Deduplication
- Business transformations

---

## Gold

- Aggregated data
- Reporting
- BI dashboards
- Machine Learning

---

# Interview Questions

## What is Structured Streaming?

A scalable streaming engine in Apache Spark that processes data continuously using micro-batches or continuous processing.

---

## Why checkpoints?

To recover from failures without losing data.

---

## Difference between Batch and Streaming

|Batch|Streaming|
|------|----------|
|Runs Once|Runs Continuously|
|Finite Data|Infinite Data|
|Scheduled|Real Time|

---

## Why use Delta?

- ACID
- Faster
- Schema Evolution
- Time Travel

---

## Why use Bronze Silver Gold?

Separates raw, cleaned, and business-ready data for easier maintenance and governance.

---

## What happens when a new JSON file arrives?

1. Spark detects the file.
2. Creates a new micro-batch.
3. Processes the file.
4. Writes to Bronze.
5. Silver cleans the data.
6. Gold updates aggregations.
7. Dashboard refreshes automatically.

---

# Real Production Enhancements

- Auto Loader (`cloudFiles`)
- Kafka as a streaming source
- Watermarking for late events
- Delta Live Tables (DLT)
- Unity Catalog
- Databricks Workflows
- Job scheduling
- Alerting and monitoring
- OPTIMIZE and ZORDER
- Expectations for data quality
- CI/CD deployment

---

# Skills Covered

- Structured Streaming
- Delta Lake
- Medallion Architecture
- Spark SQL
- DataFrames
- Streaming Aggregations
- Window Functions
- Checkpointing
- Fault Tolerance
- Real-Time ETL
- Databricks SQL
- Dashboard Integration

---

# End-to-End Pipeline Summary

```
Incoming JSON

↓

ReadStream

↓

Bronze Delta

↓

Validation

↓

Silver Delta

↓

Aggregation

↓

Gold Delta

↓

SQL Analytics

↓

Dashboard

↓

Business Users
```

---

# Learning Outcomes

After completing this project, you will be able to:

- Build real-time streaming pipelines in Databricks
- Understand Structured Streaming concepts
- Implement Medallion Architecture
- Use Delta Lake for reliable streaming
- Create business-ready Gold tables
- Monitor and troubleshoot streaming jobs
- Answer common Databricks streaming interview questions
