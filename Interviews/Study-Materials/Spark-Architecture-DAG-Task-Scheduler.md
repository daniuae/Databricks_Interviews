# Apache Spark DAG Scheduler and Task Scheduler

## Overview

Apache Spark executes applications using two major scheduling components:

1. **DAG Scheduler (High-Level Scheduler)**
2. **Task Scheduler (Low-Level Scheduler)**

Together, they convert your Spark program into executable tasks and distribute them across cluster nodes.

---

# Spark Execution Architecture

```text
                    Spark Application
                           |
                    SparkContext
                           |
                ----------------------
                |                    |
          DAG Scheduler       Task Scheduler
                |                    |
         Creates Stages       Creates Tasks
                |                    |
                ----------------------
                           |
                    Cluster Manager
      (Standalone / YARN / Kubernetes / Mesos)
                           |
                      Executors
                           |
                   Task Execution
```

---

# What is a DAG?

**DAG** stands for **Directed Acyclic Graph**.

It represents all the transformations performed on your data before Spark executes them.

- **Directed** → Execution flows in one direction.
- **Acyclic** → No loops.
- **Graph** → Operations connected together.

Example:

```scala
val sales = spark.read.csv("sales.csv")

val result = sales
    .filter($"amount" > 1000)
    .groupBy("country")
    .sum("amount")

result.show()
```

Spark internally creates the following DAG:

```text
Read CSV
    |
 Filter
    |
GroupBy
    |
 Sum
    |
 Show
```

---

# Spark Execution Flow

```text
Spark Application
        |
SparkContext Created
        |
DAG Scheduler
        |
Creates DAG
        |
Splits DAG into Stages
        |
Task Scheduler
        |
Creates Tasks
        |
Executors Execute Tasks
        |
Results Returned
```

---

# DAG Scheduler

## Definition

The **DAG Scheduler** is responsible for converting a Spark application into one or more execution stages.

It is responsible for planning the execution.

It **does not execute** any tasks.

---

# Responsibilities of DAG Scheduler

## 1. Create the DAG

Spark first analyzes all transformations.

Example:

```scala
df.filter(...)
  .select(...)
  .groupBy(...)
  .count()
```

DAG:

```text
Read
 |
Filter
 |
Select
 |
GroupBy
 |
Count
```

---

## 2. Detect Dependencies

Spark identifies dependencies between RDDs/DataFrames.

### Narrow Dependency

Each parent partition is used by only one child partition.

Examples:

- map()
- filter()
- flatMap()
- union()

Visualization:

```text
Partition 1 ---> Partition 1

Partition 2 ---> Partition 2
```

No shuffle is required.

---

### Wide Dependency

Multiple parent partitions contribute to one child partition.

Examples:

- groupBy()
- reduceByKey()
- join()
- distinct()
- orderBy()

Visualization:

```text
Partition 1 ----\
                 \
Partition 2 -------> Shuffle
                 /
Partition 3 ----/
```

Shuffle is required.

---

## 3. Split into Stages

Whenever Spark encounters a shuffle, it creates a new stage.

Example:

```text
Read
 |
Map
 |
Filter
 |
ReduceByKey
 |
Collect
```

Execution:

```text
Stage 1

Read
Map
Filter

=====================
Shuffle
=====================

Stage 2

ReduceByKey
Collect
```

---

## 4. Submit Stages

After creating stages, DAG Scheduler submits them to the Task Scheduler.

```text
Stage 1
     |
Task Scheduler

Stage 2
     |
Task Scheduler
```

---

## 5. Handle Failures

If a stage fails:

```text
Stage 1 ✔

Stage 2 ✘
```

Spark recomputes only the failed stage.

---

# DAG Scheduler Example

```scala
val sales = spark.read.csv("sales.csv")

val result = sales
    .filter($"amount" > 500)
    .groupBy("country")
    .sum("amount")

result.show()
```

Execution:

```text
Read CSV
    |
 Filter
    |
=================
Shuffle
=================
    |
GroupBy
    |
 Sum
```

Stages:

```text
Stage 1

Read
Filter

Stage 2

GroupBy
Sum
```

---

# Task Scheduler

## Definition

The **Task Scheduler** receives stages from the DAG Scheduler.

It converts each stage into multiple tasks.

Each partition becomes one task.

---

Suppose an RDD contains:

```text
8 Partitions
```

Spark creates:

```text
Task 1

Task 2

Task 3

Task 4

Task 5

Task 6

Task 7

Task 8
```

---

# Responsibilities of Task Scheduler

## 1. Create Tasks

Suppose:

```text
Stage 1

8 Partitions
```

Task Scheduler creates:

```text
Task 1

Task 2

Task 3

Task 4

Task 5

Task 6

Task 7

Task 8
```

---

## 2. Allocate Resources

The Task Scheduler checks:

- Available executors
- Available CPU cores
- Available memory

Then assigns tasks accordingly.

Example:

```text
Executor 1

Core 1
Core 2
Core 3
Core 4
```

Tasks:

```text
Task 1

Task 2

Task 3

Task 4
```

---

## 3. Optimize Data Locality

Spark tries to execute tasks on the same node where the data resides.

Example:

```text
Executor 1

Partition 1
Partition 2
```

Spark runs:

```text
Task 1

Task 2
```

on Executor 1 itself.

Benefits:

- Less network traffic
- Faster execution
- Better performance

---

## 4. Retry Failed Tasks

Suppose:

```text
Task 5

Failed
```

Spark retries only Task 5.

Other tasks continue normally.

---

## 5. Monitor Task Execution

The Task Scheduler:

- Tracks task status
- Detects failures
- Retries failed tasks
- Reports completion to the Driver

---

# Complete Execution Example

Program:

```scala
val rdd = sc.textFile("employees.txt")

val words = rdd.flatMap(_.split(" "))

val pairs = words.map(word => (word,1))

val counts = pairs.reduceByKey(_+_)

counts.collect()
```

---

## DAG

```text
Read File
     |
FlatMap
     |
Map
     |
ReduceByKey
     |
Collect
```

---

## Stage Creation

```text
Stage 1

Read
FlatMap
Map

==================
Shuffle
==================

Stage 2

ReduceByKey
Collect
```

---

## Task Creation

Suppose there are 4 partitions.

Stage 1:

```text
Task 1

Task 2

Task 3

Task 4
```

Stage 2:

```text
Task 1

Task 2

Task 3

Task 4
```

---

# Execution with Executors

Suppose:

```text
100 GB Data

20 Partitions

5 Executors

4 Cores per Executor
```

Total available cores:

```text
5 × 4 = 20 Cores
```

Spark executes:

```text
20 Tasks

↓

All run simultaneously
```

If only 10 cores exist:

```text
20 Tasks

↓

10 Tasks Run

↓

Remaining 10 Wait

↓

Execute After Resources Become Available
```

---

# End-to-End Flow

```text
Spark Program

      |
      ▼

SparkContext

      |
      ▼

DAG Scheduler

      |
      ├── Build DAG
      ├── Detect Dependencies
      ├── Identify Shuffle
      ├── Split into Stages
      └── Submit Stages

      |
      ▼

Task Scheduler

      |
      ├── Create Tasks
      ├── Allocate Executors
      ├── Schedule Tasks
      ├── Retry Failed Tasks
      └── Monitor Execution

      |
      ▼

Executors

      |
      ▼

Task Execution

      |
      ▼

Final Result
```

---

# DAG Scheduler vs Task Scheduler

| Feature | DAG Scheduler | Task Scheduler |
|----------|---------------|----------------|
| Scheduler Type | High-Level | Low-Level |
| Input | Spark DAG | Stages |
| Output | Stages | Tasks |
| Detects Shuffle | ✅ Yes | ❌ No |
| Creates Stages | ✅ Yes | ❌ No |
| Creates Tasks | ❌ No | ✅ Yes |
| Assigns Tasks to Executors | ❌ No | ✅ Yes |
| Handles Stage Failures | ✅ Yes | ❌ No |
| Retries Failed Tasks | Coordinates stage retries | ✅ Yes |
| Optimizes Data Locality | Partially | ✅ Yes |
| Directly Communicates with Executors | ❌ No | ✅ Yes |

---

# DAG Scheduler Interview Questions

### 1. What is the DAG Scheduler?

The DAG Scheduler is Spark's high-level scheduler that converts a Spark application's DAG into execution stages, detects shuffle boundaries, and submits stages to the Task Scheduler.

---

### 2. What is the Task Scheduler?

The Task Scheduler is Spark's low-level scheduler that converts stages into tasks and assigns those tasks to executors for execution.

---

### 3. What is the difference between a Stage and a Task?

| Stage | Task |
|--------|------|
| Collection of transformations | Smallest execution unit |
| Created after shuffle analysis | One task per partition |
| Managed by DAG Scheduler | Managed by Task Scheduler |

---

### 4. When is a new Stage created?

A new stage is created whenever Spark encounters a **wide transformation** that requires a **shuffle**, such as:

- groupBy()
- reduceByKey()
- join()
- distinct()
- orderBy()

---

### 5. Which scheduler communicates with executors?

The **Task Scheduler** communicates directly with executors and the cluster manager.

---

### 6. What is Data Locality?

Data locality means Spark executes a task on the same machine where the data is stored to minimize network communication and improve performance.

---

### 7. Why is the DAG Scheduler important?

The DAG Scheduler optimizes execution by:

- Building the execution graph
- Detecting dependencies
- Minimizing unnecessary shuffles
- Splitting jobs into efficient stages
- Supporting fault recovery

---

# Key Takeaways

- **DAG Scheduler** is responsible for planning execution.
- **Task Scheduler** is responsible for executing the plan.
- Every **shuffle** creates a new stage.
- Every **partition** becomes one task.
- Tasks are executed on executors.
- Spark retries only failed tasks or stages, improving fault tolerance.
- Data locality significantly improves Spark performance.

---

# Easy Way to Remember

| Component | Analogy |
|-----------|----------|
| DAG Scheduler | Project Manager (Plans the work) |
| Task Scheduler | Team Lead (Assigns work to team members) |
| Executors | Workers (Perform the actual work) |
| Stage | Phase of the project |
| Task | Individual unit of work |
| Partition | Portion of the data |

---

# Summary Diagram

```text
                    Spark Application
                           |
                      SparkContext
                           |
                   DAG Scheduler
                           |
         ---------------------------------
         |               |               |
      Stage 1         Stage 2         Stage 3
         |               |               |
         ---------------------------------
                           |
                    Task Scheduler
                           |
      ------------------------------------------
      |          |          |          |        |
    Task1      Task2      Task3      Task4   Task5
      |          |          |          |        |
   Executor1  Executor2  Executor3  Executor4 Executor5
      |          |          |          |        |
      ------------------------------------------
                           |
                     Final Output
```
