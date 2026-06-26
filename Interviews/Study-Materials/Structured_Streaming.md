# Databricks Structured Streaming - Complete Guide

# Table of Contents

1. Introduction
2. What is Streaming?
3. Batch vs Streaming
4. Real World Example
5. Streaming Architecture
6. Components of Structured Streaming
7. Reading Streams
8. Auto Loader
9. Transformations
10. Aggregations
11. Watermarks
12. Window Operations
13. Checkpointing
14. Writing Streams
15. Triggers
16. Output Modes
17. End-to-End Example
18. Complete Code Example
19. Advantages
20. Interview Questions

---

# 1. Introduction

Databricks Structured Streaming is a scalable and fault-tolerant stream processing engine built on Apache Spark. It enables processing of real-time data using the same DataFrame and SQL APIs used for batch processing.

Unlike traditional ETL jobs that process data in scheduled batches, Structured Streaming continuously processes new incoming data as soon as it arrives.

---

# 2. What is Streaming?

Streaming is the continuous processing of incoming data instead of waiting for the complete dataset.

Think of streaming like watching a live cricket match.

Instead of waiting until the match ends to know the score, you see every ball as it is bowled.

Similarly,

Instead of processing all customer orders at midnight, Databricks processes each order within seconds after it arrives.

---

# 3. Batch Processing vs Streaming

## Batch Processing

```text
            Batch Processing

CSV Files
    │
    ▼
Read Complete File
    │
    ▼
Transform
    │
    ▼
Write Output
```

Characteristics

- Runs on schedule
- High latency
- Best for historical data
- Processes finite datasets

Example

A payroll job runs once every month.

---

## Streaming Processing

```text
          Streaming Processing

Incoming Events
       │
       ▼
Structured Streaming
       │
       ▼
Transform
       │
       ▼
Delta Table
```

Characteristics

- Continuous processing
- Low latency
- Near real-time analytics
- Processes infinite datasets

Example

Online shopping orders are processed immediately.

---

# 4. Real World Example

Suppose you work for Amazon.

Customers place orders every second.

```text
09:00:01  Customer buys Laptop

↓

09:00:03  Order reaches Kafka

↓

09:00:04  Databricks reads event

↓

09:00:05  Data cleaned

↓

09:00:06  Stored into Delta Table

↓

Dashboard Updated
```

Managers can monitor sales in real time.

---

# 5. Streaming Architecture

```text
              Kafka
                │
                │
        Azure Event Hub
                │
                │
        AWS Kinesis
                │
                │
      Cloud Storage Files
                │
                ▼
      Databricks Auto Loader
                │
                ▼
    Spark Structured Streaming
                │
                ▼
        Data Transformations
                │
                ▼
          Delta Lake Tables
                │
        ┌───────┼────────┐
        ▼       ▼        ▼
   Power BI   ML Models  SQL Analytics
```

---

# 6. Components of Structured Streaming

## Source

Streaming data may come from

- Apache Kafka
- Azure Event Hub
- AWS Kinesis
- Google Pub/Sub
- Auto Loader
- Cloud Storage
- IoT Devices
- CDC
- Socket Streams

Example

```text
OrderID    Product

101        Laptop

102        Mouse

103        Keyboard
```

New records continuously arrive.

---

# 7. Reading Streams

Batch uses

```python
spark.read
```

Streaming uses

```python
spark.readStream
```

Example

```python
orders = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .load("/mnt/orders")
)
```

Spark continuously watches the folder.

Whenever a new file arrives, it is automatically processed.

---

# 8. Auto Loader

Auto Loader is a Databricks feature that efficiently detects and processes only newly arrived files.

Without Auto Loader

```text
Every execution scans

orders/

order1.json

order2.json

order3.json

order4.json
```

Every file is checked again.

With Auto Loader

```text
Already Processed

✔ order1.json

✔ order2.json

✔ order3.json

New File

order4.json

↓

Only order4.json is processed.
```

Benefits

- Incremental ingestion
- High scalability
- Automatic schema evolution
- Cost efficient

---

# 9. Transformations

Streaming transformations are identical to batch transformations.

Example

Input

```text
CustomerID   Product   Price

1            Laptop    1000

2            Mouse      -20

3            Keyboard   200
```

Transformation

```python
from pyspark.sql.functions import col

clean_orders = orders.filter(col("price") > 0)
```

Output

```text
CustomerID   Product   Price

1            Laptop    1000

3            Keyboard   200
```

---

# 10. Aggregations

Suppose incoming orders are

```text
Laptop

Laptop

Mouse

Laptop

Keyboard
```

Aggregation

```python
orders.groupBy("product").count()
```

Result

```text
Laptop      3

Mouse       1

Keyboard    1
```

---

# 11. Watermarks

Sometimes streaming data arrives late.

Example

```text
Actual Event Time

10:00 AM

Network Delay

Arrives at

10:15 AM
```

Should Spark still include this record?

Watermark decides.

Example

```python
.withWatermark("order_time","10 minutes")
```

Meaning

Accept records arriving within 10 minutes.

Older records are discarded.

Benefits

- Handles late-arriving data
- Prevents unlimited memory growth
- Required for stateful aggregations

---

# 12. Window Operations

Instead of counting total orders, calculate every minute.

Example

```text
10:00 - 10:01

120 Orders

10:01 - 10:02

145 Orders

10:02 - 10:03

98 Orders
```

Code

```python
from pyspark.sql.functions import window

orders.groupBy(
    window("order_time","1 minute")
).count()
```

Result

```text
Window                 Orders

10:00-10:01            120

10:01-10:02            145

10:02-10:03             98
```

---

# 13. Checkpointing

Streaming jobs should recover after failures.

Checkpoint stores

- Offsets
- Metadata
- State Information
- Processed Files

Example

```python
.option(
    "checkpointLocation",
    "/mnt/checkpoint/orders"
)
```

Recovery

```text
Cluster Failure

↓

Restart Job

↓

Read Checkpoint

↓

Continue Processing
```

Benefits

- Fault tolerance
- Exactly-once processing
- Recovery without duplicates

---

# 14. Writing Streams

Batch uses

```python
write
```

Streaming uses

```python
writeStream
```

Example

```python
query = (
    clean_orders.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            "/mnt/checkpoint/orders"
        )
        .start("/mnt/delta/orders")
)
```

---

# 15. Triggers

Triggers decide how frequently Spark processes incoming data.

Example

Every five seconds

```python
.trigger(processingTime="5 seconds")
```

Timeline

```text
10:00:05

↓

Process Batch

↓

10:00:10

↓

Process Batch

↓

10:00:15

↓

Process Batch
```

---

# 16. Output Modes

## Append Mode

Writes only new rows.

Example

```text
Existing

A

B

C

New

D

Output

D
```

---

## Complete Mode

Writes the entire result table every trigger.

```text
Output

A

B

C

D
```

---

## Update Mode

Writes only changed rows.

Useful for aggregations.

---

# 17. End-to-End Streaming Example

Architecture

```text
Customer Orders

↓

Cloud Storage

↓

Auto Loader

↓

readStream

↓

Transformations

↓

writeStream

↓

Delta Table

↓

Power BI Dashboard
```

---

# 18. Complete Code Example

```python
from pyspark.sql.functions import col

# Read streaming data
orders = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .load("/mnt/orders")
)

# Transform data
clean_orders = (
    orders
    .filter(col("price") > 0)
    .withColumn(
        "total_amount",
        col("price") * col("quantity")
    )
)

# Write streaming data
(
    clean_orders.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/mnt/checkpoints/orders"
        )
        .start("/mnt/delta/orders")
)
```

---

# 19. Advantages of Structured Streaming

- Same API for batch and streaming
- Highly scalable
- Fault tolerant
- Supports exactly-once processing
- Automatic recovery
- Handles late-arriving data
- Works seamlessly with Delta Lake
- Supports SQL, Python, Scala, and Java
- Supports real-time dashboards
- Integrates with Kafka, Kinesis, Event Hub, and Auto Loader

---

# 20. Complete Flow Diagram

```text
              Source Systems
                    │
     ┌──────────────┼──────────────┐
     │              │              │
   Kafka       Cloud Storage     Event Hub
     │              │              │
     └──────────────┼──────────────┘
                    │
                    ▼
          Databricks Auto Loader
                    │
                    ▼
         Spark Structured Streaming
                    │
         ┌──────────┼──────────┐
         │          │          │
      Filter      Join     Aggregate
         │          │          │
         └──────────┼──────────┘
                    │
                    ▼
              Delta Lake
                    │
      ┌─────────────┼─────────────┐
      ▼             ▼             ▼
  Power BI     SQL Analytics   ML Models
```

---

# Interview Questions

### 1. What is Structured Streaming?

Structured Streaming is Apache Spark's scalable stream processing engine that treats streaming data as an unbounded table and processes new data incrementally using DataFrame APIs.

---

### 2. What is Auto Loader?

Auto Loader is a Databricks feature that incrementally discovers and processes newly arrived files without scanning the entire storage location repeatedly.

---

### 3. Why is checkpointing required?

Checkpointing stores metadata, offsets, and state information so that a streaming query can recover from failures without data loss or duplicate processing.

---

### 4. What is watermarking?

Watermarking defines how long Spark waits for late-arriving data before discarding it during stateful operations such as aggregations and joins.

---

### 5. What are the output modes?

- Append
- Update
- Complete

---

### 6. Difference between Batch and Streaming

| Batch | Streaming |
|--------|-----------|
| Finite data | Infinite data |
| Scheduled execution | Continuous execution |
| High latency | Low latency |
| Historical analysis | Real-time analysis |
| Uses `spark.read` | Uses `spark.readStream` |

---

# Summary

Databricks Structured Streaming enables organizations to build reliable, scalable, and real-time data pipelines. Using features such as Auto Loader, Delta Lake, checkpointing, watermarking, and window-based aggregations, businesses can process millions of events continuously with fault tolerance and exactly-once processing guarantees.
