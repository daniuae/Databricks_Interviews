# Databricks Essentials – Volume 1

## Table of Contents
1. Introduction to Databricks
2. Workspace Overview
3. Workspace Objects
4. Cluster Management
5. Cluster Policies
6. User Roles & Permissions
7. Workspace Security
8. Personal Access Tokens
9. Databricks Repos
10. Notebooks (Python, SQL, Scala)
11. DBFS Architecture
12. File System Operations
13. Mounting External Storage
14. Databricks CLI Basics
15. Hands-on Labs
16. Interview Questions

---

# 1. Introduction

Databricks is a unified analytics platform built on Apache Spark. It provides collaborative notebooks, scalable clusters, Delta Lake, ML, SQL Warehouses, governance, and workflow orchestration.

## Lakehouse Architecture (Illustration)

```text
        Data Sources
             |
      Batch / Streaming
             |
      +----------------+
      |  Bronze Layer  |
      +----------------+
             |
      +----------------+
      |  Silver Layer  |
      +----------------+
             |
      +----------------+
      |   Gold Layer   |
      +----------------+
             |
      BI / AI / ML / SQL
```

# 2. Workspace Overview

A Databricks Workspace is the primary collaborative environment.

Components:
- Notebooks
- Repos
- Compute (Clusters)
- Workflows
- SQL
- Catalog Explorer

# 3. Workspace Objects

- Folders
- Notebooks
- Libraries
- Dashboards
- Files
- Repos

# 4. Cluster Management

Cluster Types:
- All-purpose
- Job Cluster

Python example:

```python
df = spark.read.csv("/FileStore/data/employees.csv", header=True)
df.show()
```

SQL example:

```sql
SELECT * FROM employees;
```

Scala example:

```scala
val df = spark.read.option("header","true").csv("/FileStore/data/employees.csv")
display(df)
```

# 5. DBFS

DBFS (Databricks File System) is a distributed abstraction over cloud storage.

Useful commands:

```python
display(dbutils.fs.ls("/"))
dbutils.fs.mkdirs("/demo")
dbutils.fs.rm("/demo", recurse=True)
```

# 6. Databricks CLI

Example:

```bash
databricks auth login
databricks workspace list
databricks fs ls dbfs:/
```

# 7. Hands-on Lab

Objective:
- Create a workspace folder
- Upload a CSV
- Read it with Spark
- Save as Delta

```python
df = spark.read.option("header",True).csv("/FileStore/data/employees.csv")
df.write.format("delta").mode("overwrite").save("/tmp/employees_delta")
```

# 8. Interview Questions

1. What is a Databricks Workspace?
2. Difference between Job and All-purpose clusters?
3. What is DBFS?
4. What are Repos?
5. What is a Personal Access Token?
