# Databricks Basics Interview Questions and Answers

> **Interview Preparation Guide**
>
> **Target Audience**
>
> * Data Engineer
> * Senior Data Engineer
> * Lead Data Engineer
> * Databricks Developer
> * Spark Developer
> * Data Engineering Trainer

---

# 1. What is Databricks?

## Answer

Databricks is a **cloud-based unified analytics platform** built on **Apache Spark**.

It provides a single platform for:

* Data Engineering
* Data Science
* Machine Learning
* Data Analytics
* SQL Analytics

It supports:

* AWS
* Azure
* Google Cloud Platform (GCP)

### Interview Answer (30 Seconds)

> Databricks is a cloud-native Lakehouse platform built on Apache Spark that unifies data engineering, data science, machine learning, and analytics. It simplifies large-scale data processing with managed Spark clusters, Delta Lake, collaborative notebooks, and automated workflows.

---

# 2. Why do companies use Databricks?

## Answer

Companies use Databricks because it provides:

* Managed Apache Spark
* Auto Scaling
* Auto Termination
* Delta Lake
* Collaborative Notebooks
* Workflow Scheduling
* Machine Learning Support
* SQL Analytics
* Streaming
* Enterprise Security

---

# 3. What problems does Databricks solve?

Traditional Big Data challenges:

* Complex Spark installation
* Hadoop cluster management
* Dependency conflicts
* Difficult scaling
* Infrastructure maintenance

Databricks solves these through:

* Managed Spark Clusters
* Auto Scaling
* Cloud-native Architecture
* Delta Lake
* Interactive Notebooks
* Workflow Automation

---

# 4. What is Lakehouse Architecture?

```
        Data Lake
            +
     Data Warehouse
            =
        Lakehouse
```

Benefits:

* Low-cost storage
* ACID transactions
* BI reporting
* Machine Learning
* Streaming
* Batch processing

---

# 5. Difference between Data Lake and Data Warehouse

| Data Lake            | Data Warehouse         |
| -------------------- | ---------------------- |
| Stores raw data      | Stores processed data  |
| Schema-on-read       | Schema-on-write        |
| Low-cost storage     | Expensive storage      |
| Supports all formats | Mostly structured data |
| Good for ML          | Good for BI            |

---

# 6. What is Delta Lake?

## Answer

Delta Lake is an open-source storage layer that brings reliability to data lakes.

### Features

* ACID Transactions
* Time Travel
* Schema Enforcement
* Schema Evolution
* MERGE
* UPDATE
* DELETE

### Interview Answer

> Delta Lake is an open-source storage framework that provides ACID transactions, schema management, version history, and scalable metadata on top of cloud object storage.

---

# 7. Delta Lake vs Parquet

| Parquet            | Delta Lake     |
| ------------------ | -------------- |
| No ACID            | ACID Supported |
| No Time Travel     | Time Travel    |
| No Version History | Versioning     |
| No Merge           | Merge          |
| No Update          | Update         |
| No Delete          | Delete         |

---

# 8. What is Apache Spark?

Apache Spark is a distributed processing engine for big data.

It supports:

* Batch Processing
* Streaming
* SQL
* Machine Learning
* Graph Processing

---

# 9. Why is Spark Fast?

Spark is fast because of:

* In-Memory Processing
* Parallel Execution
* DAG Optimization
* Catalyst Optimizer
* Tungsten Execution Engine

---

# 10. What are Databricks Notebooks?

Databricks notebooks support:

* Python
* SQL
* Scala
* R

Features:

* Collaboration
* Visualization
* Scheduling
* Version Control

---

# 11. What is a Cluster?

A cluster is a group of virtual machines that execute Spark applications.

Components:

* Driver Node
* Worker Nodes

---

# 12. Driver Node vs Worker Node

| Driver              | Worker               |
| ------------------- | -------------------- |
| Coordinates the job | Executes tasks       |
| Creates DAG         | Processes partitions |
| One                 | Multiple             |

---

# 13. What is Auto Scaling?

Automatically increases or decreases worker nodes according to workload.

Benefits:

* Better performance
* Lower cloud cost

---

# 14. What is Auto Termination?

Automatically shuts down idle clusters.

Benefits:

* Cost optimization
* Prevents idle resources

---

# 15. What is DBFS?

DBFS (Databricks File System) is a unified interface that allows access to cloud storage such as:

* Amazon S3
* Azure Data Lake Storage
* Google Cloud Storage

---

# 16. What is dbutils?

Databricks utility library.

Used for:

* File operations
* Secrets
* Widgets
* Notebook execution

Example:

```python
dbutils.fs.ls("/FileStore")
```

---

# 17. What are Widgets?

Widgets allow notebooks to accept user input.

Example:

```python
dbutils.widgets.text("country","India")
```

Retrieve value:

```python
dbutils.widgets.get("country")
```

---

# 18. What is Unity Catalog?

Unity Catalog is Databricks' centralized governance solution.

Features:

* Centralized permissions
* Data lineage
* Auditing
* Fine-grained access control

---

# 19. What is Data Lineage?

Tracks:

* Source
* Transformations
* Destination

Benefits:

* Auditing
* Debugging
* Compliance

---

# 20. What is a Workspace?

A workspace contains:

* Notebooks
* Clusters
* Jobs
* Libraries
* Repositories

---

# 21. What is a Job?

A Job automates execution of:

* Notebook
* Python Script
* SQL Query
* JAR

Supports scheduling:

* Hourly
* Daily
* Weekly
* Monthly

---

# 22. What are Workflows?

Workflows orchestrate multiple dependent tasks.

Example:

```
Notebook A
     ↓
Notebook B
     ↓
Notebook C
```

---

# 23. What is Databricks Repos?

Repos integrate Git with Databricks.

Supported providers:

* GitHub
* GitLab
* Azure DevOps
* Bitbucket

---

# 24. What is Photon?

Photon is Databricks' high-performance vectorized execution engine.

Benefits:

* Faster SQL
* Faster Delta operations
* Lower cloud costs

---

# 25. What is Time Travel?

Allows querying previous versions of Delta tables.

Example:

```sql
SELECT *
FROM sales VERSION AS OF 5;
```

---

# 26. What is VACUUM?

VACUUM removes obsolete files from Delta tables after the retention period.

Benefits:

* Saves storage
* Improves performance

---

# 27. What is OPTIMIZE?

OPTIMIZE compacts many small files into fewer large files.

Benefits:

* Faster reads
* Better performance

---

# 28. What is ZORDER?

ZORDER colocates related data together.

Example:

```sql
OPTIMIZE sales
ZORDER BY(customer_id);
```

Benefits:

* Better data skipping
* Faster filtering

---

# 29. What is Schema Enforcement?

Ensures incoming data matches the existing table schema.

Invalid data is rejected.

---

# 30. What is Schema Evolution?

Allows compatible schema changes such as adding new columns.

---

# 31. Schema Enforcement vs Schema Evolution

| Schema Enforcement     | Schema Evolution                  |
| ---------------------- | --------------------------------- |
| Rejects invalid schema | Accepts compatible schema changes |
| Maintains consistency  | Allows controlled evolution       |

---

# 32. What is Medallion Architecture?

```
Bronze
   ↓
Silver
   ↓
Gold
```

### Bronze

Raw data.

### Silver

Cleaned and validated data.

### Gold

Business-ready data.

---

# 33. What is a Secret Scope?

Secure storage for:

* Passwords
* API Keys
* Tokens
* Secrets

---

# 34. What is Cache?

Stores frequently used data in memory.

Benefits:

* Faster processing
* Reduced disk I/O

---

# 35. Cache vs Persist

| Cache       | Persist                 |
| ----------- | ----------------------- |
| Memory only | Memory and/or disk      |
| Simple      | Flexible storage levels |

---

# 36. What is Lazy Evaluation?

Spark delays execution until an action is called.

Example:

```python
df.filter(col("salary") > 50000)
```

Nothing executes yet.

```python
df.show()
```

Execution starts.

---

# 37. What are Transformations?

Transformations create new DataFrames.

Examples:

```python
filter()
select()
join()
groupBy()
orderBy()
```

---

# 38. What are Actions?

Actions trigger execution.

Examples:

```python
show()
count()
collect()
write()
```

---

# 39. What is DAG?

DAG stands for **Directed Acyclic Graph**.

Spark builds a DAG to optimize execution before running tasks.

---

# 40. What is Catalyst Optimizer?

Spark SQL's query optimizer.

Optimizes:

* Predicate Pushdown
* Join Reordering
* Projection Pruning
* Constant Folding

---

# 41. What is Predicate Pushdown?

Filters data at the storage layer before reading it.

Benefits:

* Less I/O
* Faster queries

---

# 42. What is Partitioning?

Partitioning divides data into smaller pieces for parallel processing.

Benefits:

* Scalability
* Better performance

---

# 43. What is Repartition?

Redistributes data across partitions.

Characteristics:

* Full shuffle
* Can increase or decrease partitions

---

# 44. What is Coalesce?

Reduces the number of partitions while minimizing data movement.

Usually used after filtering large datasets.

---

# 45. Repartition vs Coalesce

| Repartition                  | Coalesce                   |
| ---------------------------- | -------------------------- |
| Full shuffle                 | Minimal shuffle            |
| Increase/decrease partitions | Mostly decrease partitions |
| Higher cost                  | Lower cost                 |

---

# 46. What is Broadcast Join?

Broadcasts a small table to all worker nodes.

Benefits:

* Avoids large shuffles
* Faster joins

---

# 47. Explain ACID Transactions in Delta Lake

ACID stands for:

* Atomicity
* Consistency
* Isolation
* Durability

Delta Lake guarantees ACID compliance.

---

# 48. Explain Bronze, Silver, and Gold Layers

| Layer  | Purpose                         |
| ------ | ------------------------------- |
| Bronze | Raw data                        |
| Silver | Cleansed and validated data     |
| Gold   | Aggregated, business-ready data |

---

# 49. Advantages of Databricks

* Unified analytics platform
* Managed Spark
* Delta Lake
* Auto Scaling
* Auto Termination
* Collaborative notebooks
* Machine Learning support
* SQL Analytics
* Streaming
* Enterprise-grade security

---

# 50. Most Frequently Asked Interview Questions

1. What is Databricks?
2. Explain Lakehouse Architecture.
3. What is Delta Lake?
4. Difference between Delta and Parquet.
5. Explain Medallion Architecture.
6. What is Unity Catalog?
7. What is Photon?
8. Explain Time Travel.
9. What is OPTIMIZE?
10. What is VACUUM?
11. Explain ZORDER.
12. Difference between Repartition and Coalesce.
13. What is Broadcast Join?
14. Explain Lazy Evaluation.
15. What are Transformations and Actions?
16. Explain DAG.
17. Explain Catalyst Optimizer.
18. What is Predicate Pushdown?
19. How do you optimize a Databricks job?
20. What are the advantages of Databricks?

---

# Rapid Fire (One-Line Answers)

| Question       | Answer                                                |
| -------------- | ----------------------------------------------------- |
| Databricks     | Cloud-native Lakehouse platform built on Apache Spark |
| Delta Lake     | Storage layer with ACID transactions and versioning   |
| DBFS           | Interface to cloud object storage                     |
| Unity Catalog  | Centralized governance for data assets                |
| Photon         | High-performance execution engine                     |
| Time Travel    | Query previous table versions                         |
| OPTIMIZE       | Compacts small files                                  |
| VACUUM         | Removes obsolete files                                |
| ZORDER         | Improves query performance                            |
| Broadcast Join | Sends a small table to all worker nodes               |

---

# Interview Tips

* Keep answers within **30–60 seconds**.
* Explain **What**, **Why**, and **When**.
* Use real-world examples whenever possible.
* For senior roles, be ready to discuss:

  * Spark execution plans
  * Performance tuning
  * Delta optimization
  * Unity Catalog governance
  * CI/CD pipelines
  * Monitoring and troubleshooting
  * Cost optimization
