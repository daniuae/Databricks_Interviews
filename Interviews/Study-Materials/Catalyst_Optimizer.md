# Tungsten Execution Engine (Apache Spark)

## What is Tungsten?

The **Tungsten Execution Engine** is Spark's **low-level execution engine** that focuses on making Spark jobs run faster by optimizing **CPU usage, memory management, and code execution**.

If **Catalyst Optimizer** decides **what** is the best execution plan, **Tungsten** decides **how** to execute that plan as efficiently as possible.

---

# Simple Analogy

Imagine you're delivering packages.

- **Catalyst** is the logistics manager who decides the best route.
- **Tungsten** is the engineer who builds a faster truck with better fuel efficiency and optimized loading.

```text
SQL / DataFrame

       │

       ▼

Catalyst Optimizer
(Chooses Best Plan)

       │

       ▼

Tungsten Execution Engine
(Executes Efficiently)

       │

       ▼

Executors
```

---

# Why Was Tungsten Introduced?

Before Spark 1.4, Spark relied heavily on:

- Java Objects
- JVM Garbage Collection (GC)
- Object Serialization
- Reflection
- Inefficient memory usage

Problems included:

- High Garbage Collection pauses
- Excessive memory consumption
- Slow execution
- CPU inefficiency
- Large serialization overhead

Tungsten was introduced (around Spark 1.4) to solve these performance bottlenecks.

---

# Spark Architecture with Tungsten

```text
            SQL / DataFrame

                    │

                    ▼

          Catalyst Optimizer

                    │

                    ▼

         Physical Execution Plan

                    │

                    ▼

        Tungsten Execution Engine

     ┌───────────┬──────────────┐
     │           │              │
 Memory Mgmt   CodeGen     CPU Optimization
     │           │              │
     └───────────┴──────────────┘

                    │

                    ▼

              Spark Executors
```

---

# Main Features of Tungsten

Tungsten introduced several major improvements:

1. Off-Heap Memory Management
2. Binary Memory Format
3. Whole-Stage Code Generation
4. Efficient Serialization
5. CPU Optimization
6. Optimized Shuffle

---

# 1. Off-Heap Memory Management

## Before Tungsten

Spark stored everything as Java objects.

```text
Employee Object

↓

JVM Heap

↓

Garbage Collection
```

Problems:

- Millions of Java objects
- Frequent GC
- Memory fragmentation
- Slower applications

---

## After Tungsten

Spark stores data outside the JVM Heap.

```text
JVM Heap

↓

Minimal Objects

↓

Off-Heap Memory

↓

Binary Data
```

### Advantages

- Less Garbage Collection
- Better memory utilization
- Faster execution
- Reduced object overhead

---

# 2. Binary Memory Format

Instead of storing Java objects like:

```java
class Employee {

    int id;

    String name;

    double salary;

}
```

Spark stores records in compact binary format.

Conceptually:

```text
010101001010101010010101
```

### Benefits

- Lower memory usage
- Better CPU cache utilization
- Faster comparisons
- Improved scan performance

---

# 3. Whole-Stage Code Generation

One of Tungsten's biggest optimizations.

## Without Whole-Stage Code Generation

```text
Read

↓

Filter

↓

Map

↓

Aggregate

↓

Write
```

Each operator executes independently.

Problems:

- Multiple function calls
- Intermediate objects
- Increased CPU overhead

---

## With Whole-Stage Code Generation

Spark combines compatible operators into a single generated Java function.

```text
Read
   │
Filter
   │
Map
   │
Aggregate
   │
Write

↓

One Optimized Java Function
```

### Benefits

- Fewer virtual function calls
- Better CPU pipeline utilization
- Lower overhead
- Faster execution

---

## Example

```python
df.filter(df.salary > 50000) \
  .select("department", "salary") \
  .groupBy("department") \
  .avg()
```

Instead of executing every transformation independently, Spark generates optimized Java bytecode to execute them efficiently.

---

# 4. Efficient Serialization

## Traditional Execution

```text
Java Object

↓

Serialize

↓

Network

↓

Deserialize

↓

Java Object
```

Serialization consumes CPU and memory.

---

## Tungsten

Uses compact binary representation.

Advantages:

- Less network traffic
- Faster serialization
- Faster shuffle
- Reduced memory footprint

---

# 5. CPU Optimization

Modern CPUs are extremely fast.

Spark optimizes execution to fully utilize CPU caches.

Techniques include:

- Better CPU cache locality
- Reduced pointer chasing
- Tight execution loops
- Fewer object allocations

Result:

```text
CPU

↓

Less Waiting

↓

More Processing

↓

Higher Throughput
```

---

# 6. Optimized Shuffle

Shuffle is one of the costliest Spark operations.

Example:

```python
df.groupBy("department")
```

Without Tungsten:

- Large Java objects
- Heavy serialization
- More disk spills
- Higher network traffic

With Tungsten:

- Binary data
- Compact serialization
- Less memory usage
- Faster shuffle

---

# Execution Flow

Suppose we execute:

```sql
SELECT department,
       AVG(salary)
FROM employee
WHERE salary > 50000
GROUP BY department;
```

Execution pipeline:

```text
SQL

↓

Catalyst

↓

Optimized Physical Plan

↓

Tungsten

↓

Generated Java Code

↓

CPU

↓

Result
```

---

# Catalyst vs Tungsten

| Catalyst Optimizer | Tungsten Execution Engine |
|--------------------|---------------------------|
| Optimizes queries | Executes queries |
| Rule-Based Optimization | Memory Optimization |
| Cost-Based Optimization | CPU Optimization |
| Chooses Join Strategy | Executes Join Efficiently |
| Creates Physical Plan | Executes Physical Plan |
| Before Execution | During Execution |

---

# JVM vs Tungsten

## Without Tungsten

```text
Java Objects

↓

JVM Heap

↓

Garbage Collection

↓

Execution
```

---

## With Tungsten

```text
Binary Data

↓

Off-Heap Memory

↓

Minimal Garbage Collection

↓

Fast Execution
```

---

# Tungsten Components

```text
                Tungsten

      ┌─────────┼──────────┐

      │         │          │

 Off-Heap   CodeGen    Binary Format

      │         │          │

 CPU Cache  Less GC   Less Memory

      │         │          │

      └─────────┼──────────┘

                │

        Faster Spark Jobs
```

---

# Benefits of Tungsten

- Reduced Garbage Collection
- Lower Memory Usage
- Better CPU Utilization
- Faster Joins
- Faster Aggregations
- Faster Sorting
- Faster Shuffle
- Higher Throughput
- Better Scalability

---

# Real Spark Example

```python
employees = spark.read.parquet("/data/employees")

result = (
    employees
        .filter("salary > 50000")
        .select("department", "salary")
        .groupBy("department")
        .avg("salary")
)
```

### Catalyst Optimizer

- Pushes filters to the data source
- Reads only required columns
- Chooses the best aggregation strategy
- Creates the optimized physical plan

### Tungsten Execution Engine

- Stores rows in binary format
- Uses Off-Heap Memory
- Generates optimized Java code
- Minimizes Garbage Collection
- Executes aggregations efficiently

---

# Catalyst + Tungsten + AQE

```text
          SQL / DataFrame

                 │

                 ▼

       Catalyst Optimizer
     (Query Optimization)

                 │

                 ▼

       Physical Execution Plan

                 │

                 ▼

      Tungsten Execution Engine
 (Memory, CPU, Code Generation)

                 │

                 ▼

 Adaptive Query Execution (AQE)
(Runtime Optimizations)

                 │

                 ▼

          Spark Executors

                 │

                 ▼

              Final Result
```

---

# Comparison

| Component | Responsibility | Phase |
|-----------|---------------|-------|
| Catalyst Optimizer | Query Optimization | Before Execution |
| Tungsten | Memory, CPU and Code Optimization | During Execution |
| Adaptive Query Execution (AQE) | Runtime Optimization | During Execution |

---

# Interview Questions

## 1. What is Tungsten?

Tungsten is Spark's execution engine that improves runtime performance by optimizing memory management, CPU utilization, serialization, binary processing, and code generation.

---

## 2. Why was Tungsten introduced?

To overcome JVM limitations such as:

- Heavy Garbage Collection
- Large Java Objects
- Serialization Overhead
- Poor CPU Utilization

---

## 3. What are the major features of Tungsten?

- Off-Heap Memory
- Binary Memory Format
- Whole-Stage Code Generation
- Efficient Serialization
- CPU Cache Optimization
- Optimized Shuffle

---

## 4. Difference between Catalyst and Tungsten?

| Catalyst | Tungsten |
|----------|-----------|
| Query Optimizer | Execution Engine |
| Creates Optimized Plans | Executes Plans Efficiently |
| Rule-Based & Cost-Based | Memory & CPU Optimizations |
| Works Before Execution | Works During Execution |

---

## 5. How do Catalyst, Tungsten and AQE work together?

1. **Catalyst** analyzes and optimizes SQL/DataFrame queries.
2. **Tungsten** executes the optimized plan using efficient memory management, CPU optimizations, and generated code.
3. **Adaptive Query Execution (AQE)** monitors execution and dynamically adjusts the physical plan based on runtime statistics.

---

# Key Takeaways

- **Catalyst** decides **what** execution plan Spark should use.
- **Tungsten** decides **how** to execute that plan as efficiently as possible.
- **AQE** can modify the execution strategy during runtime using real execution statistics.
- Together, Catalyst, Tungsten, and AQE are the foundation of Spark's high-performance query engine.
