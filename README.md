# PySpark Interview Practice

A hands-on **PySpark interview preparation project** covering common Data Engineer coding patterns using a realistic employee dataset.

The project focuses on practical PySpark operations commonly tested in **Data Engineering interviews**, including transformations, aggregations, window functions, deduplication, arrays, partitioning, Delta Lake, and Spark performance analysis.

---

## Skills Covered

* DataFrame filtering and column selection
* `withColumn()`, `when()`, and `otherwise()`
* NULL handling
* Aggregations with `groupBy()` and `agg()`
* Sorting and ordering
* Window functions and Top-N
* Deduplication using the latest timestamp
* Arrays with `split()`, `explode()`, and `array_contains()`
* Date transformations
* `repartition()` vs `coalesce()`
* Delta Lake writes
* Spark physical plans
* Shuffle / Exchange detection using `explain()`

---

## Repository Structure

```text
pyspark-interview-practice/
├── README.md
├── data/
│   └── employees_pyspark_practice.csv
└── notebooks/
    └── pyspark_interview_questions.py
```

---

## Dataset

The practice dataset contains:

* Employee ID
* Name
* Department
* Salary
* Age
* City
* Joining date
* Manager ID
* Skills
* Status
* Update timestamp

The dataset intentionally includes **NULL salaries** and **duplicate employee IDs with different update timestamps** to simulate common data-quality scenarios encountered in real-world data pipelines.

---

# Databricks Setup

Upload:

```text
data/employees_pyspark_practice.csv
```

to a Databricks Volume.

Then update `csv_path` with your Volume path.

```python
csv_path = "/Volumes/<catalog>/<schema>/<volume>/employees_pyspark_practice.csv"
```

Load the dataset:

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

df.show()
df.printSchema()
```

---

# PySpark Interview Questions & Solutions

First import the commonly used functions:

```python
from pyspark.sql.functions import (
    col,
    when,
    avg,
    max,
    count,
    row_number,
    split,
    explode,
    array_contains,
    current_date,
    months_between
)

from pyspark.sql.window import Window
```

---

## 1. Filter employees with salary > 60,000

```python
df.filter(
    col("salary") > 60000
).show()
```

**Interview concept:** `filter()` selects rows based on a condition.

---

## 2. Select name, department, and salary

```python
df.select(
    "name",
    "department",
    "salary"
).show()
```

**Interview concept:** `select()` performs column selection/projection.

---

## 3. Find Data Engineering employees with salary > 70,000

```python
df.filter(
    (col("department") == "Data Engineering") &
    (col("salary") > 70000)
).show()
```

**Interview concept:** Use `&` for AND conditions between PySpark Column expressions.

---

## 4. Create a salary category

```python
df = df.withColumn(
    "salary_category",
    when(col("salary") >= 75000, "High")
    .otherwise("Normal")
)

df.show()
```

**Interview concept:** `withColumn()` creates or replaces a column, while `when().otherwise()` provides conditional logic.

---

## 5. Find NULL salaries

```python
df.filter(
    col("salary").isNull()
).show()
```

**Interview concept:** Use `isNull()` instead of comparing a column with `== None`.

---

## 6. Replace NULL salaries with 0

```python
df = df.fillna({
    "salary": 0
})

df.show()
```

**Interview concept:** `fillna()` replaces missing values.

---

# Aggregations & Sorting

## 7. Count employees by department

```python
df.groupBy("department") \
  .agg(
      count("*").alias("employee_count")
  ) \
  .show()
```

Alternative:

```python
df.groupBy("department").count().show()
```

**Interview concept:** `groupBy()` groups records before applying an aggregation.

---

## 8. Calculate average salary by department

```python
df.groupBy("department") \
  .agg(
      avg("salary").alias("avg_salary")
  ) \
  .show()
```

**Interview concept:** Aggregations such as `avg()`, `sum()`, `max()`, and `count()` are commonly used with `groupBy()`.

---

## 9. Find maximum salary by department

```python
df.groupBy("department") \
  .agg(
      max("salary").alias("max_salary")
  ) \
  .show()
```

---

## 10. Sort by salary descending

```python
df.orderBy(
    col("salary").desc()
).show()
```

Alternative:

```python
df.sort(
    col("salary").desc()
).show()
```

---

# Window Functions & Deduplication

## 11. Find the top 3 highest-paid employees per department

Create a window:

```python
window_spec = (
    Window
    .partitionBy("department")
    .orderBy(col("salary").desc())
)
```

Apply `row_number()`:

```python
top_3 = (
    df.withColumn(
        "rn",
        row_number().over(window_spec)
    )
    .filter(col("rn") <= 3)
)

top_3.show()
```

**Interview concept:**

```text
partitionBy("department")
        ↓
Create separate logical groups

orderBy(salary DESC)
        ↓
Highest salary first

row_number()
        ↓
1, 2, 3...

filter <= 3
        ↓
Top 3 per department
```

---

## 12. Deduplicate employee IDs and retain the latest record

Suppose the same `employee_id` appears multiple times with different `update_timestamp` values.

```python
dedup_window = (
    Window
    .partitionBy("employee_id")
    .orderBy(col("update_timestamp").desc())
)
```

Then:

```python
latest_df = (
    df.withColumn(
        "rn",
        row_number().over(dedup_window)
    )
    .filter(col("rn") == 1)
    .drop("rn")
)

latest_df.show()
```

**Interview concept:** The newest record gets `row_number = 1`, so older duplicates can be removed.

---

# Arrays

## 13. Convert skills into an array

Suppose:

```text
Python,Spark,SQL
```

Convert it to:

```text
["Python", "Spark", "SQL"]
```

Using:

```python
skills_df = df.withColumn(
    "skills_array",
    split(col("skills"), ",")
)

skills_df.show(truncate=False)
```

---

## 14. Explode skills into separate rows

```python
exploded_df = skills_df.withColumn(
    "skill",
    explode(col("skills_array"))
)

exploded_df.select(
    "employee_id",
    "name",
    "skill"
).show()
```

Example:

```text
Before:

Aditya → [Python, Spark, SQL]

After:

Aditya → Python
Aditya → Spark
Aditya → SQL
```

**Interview concept:** `explode()` converts array elements into separate rows.

---

## 15. Find employees with Spark as a skill

```python
skills_df.filter(
    array_contains(col("skills_array"), "Spark")
).show()
```

**Interview concept:** `array_contains()` checks whether an array contains a particular value.

---

# Date Transformations

## 16. Calculate years of service

```python
service_df = df.withColumn(
    "years_of_service",
    (
        months_between(
            current_date(),
            col("joining_date")
        ) / 12
    ).cast("int")
)

service_df.select(
    "name",
    "joining_date",
    "years_of_service"
).show()
```

**Interview concept:** Calculate the difference between the current date and joining date and convert it into years.

---

# Partitioning

## 17. Repartition by department

```python
repartitioned_df = df.repartition(
    "department"
)
```

Check partitions:

```python
print(repartitioned_df.rdd.getNumPartitions())
```

**Interview concept:** `repartition()` redistributes data across partitions and normally causes a **shuffle**.

---

## 18. Compare repartition with coalesce

```python
repartitioned_df = df.repartition(8)

coalesced_df = repartitioned_df.coalesce(4)
```

Check:

```python
print(
    "Repartition:",
    repartitioned_df.rdd.getNumPartitions()
)

print(
    "Coalesce:",
    coalesced_df.rdd.getNumPartitions()
)
```

### Key difference

| `repartition()`                     | `coalesce()`                         |
| ----------------------------------- | ------------------------------------ |
| Can increase or decrease partitions | Normally used to decrease partitions |
| Performs redistribution/shuffle     | Usually avoids a full shuffle        |
| More even distribution              | Can result in uneven partitions      |
| More expensive                      | Usually cheaper                      |

**Interview answer:**
`repartition()` is useful when I need better data distribution, while `coalesce()` is useful when I mainly want to reduce the number of partitions without a full redistribution.

---

# Delta Lake

## 19. Write cleaned data as Delta

```python
delta_path = "/Volumes/<catalog>/<schema>/<volume>/employees_delta"
```

Write:

```python
latest_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_path)
```

Read it:

```python
delta_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

delta_df.show()
```

**Interview concept:** Delta Lake stores data using Parquet files while adding a transaction log for features such as ACID transactions, schema enforcement/evolution, and time travel.

---

# Performance Analysis

## 20. Inspect a physical plan and identify Shuffle / Exchange

Create an aggregation:

```python
result = (
    df.groupBy("department")
    .agg(
        avg("salary").alias("avg_salary")
    )
)
```

Inspect the execution plan:

```python
result.explain()
```

Or:

```python
result.explain("formatted")
```

Look for:

```text
Exchange
```

An `Exchange` in the Spark physical plan generally indicates data redistribution between partitions.

For example:

```text
groupBy("department")
        ↓
Same departments need to come together
        ↓
Data redistributed
        ↓
Exchange / Shuffle
        ↓
Aggregation
```

**Interview answer:**
Operations such as `groupBy`, joins, `distinct`, `repartition`, and global sorting can cause shuffles because Spark may need to redistribute data between partitions.

---

# Quick Interview Revision

| Operation        | Purpose                          | Shuffle?                                       |
| ---------------- | -------------------------------- | ---------------------------------------------- |
| `filter()`       | Filter rows                      | Usually No                                     |
| `select()`       | Select columns                   | No                                             |
| `withColumn()`   | Add/modify column                | Usually No                                     |
| `groupBy()`      | Group records                    | Usually Yes                                    |
| `distinct()`     | Remove duplicate rows            | Usually Yes                                    |
| `orderBy()`      | Global sorting                   | Usually Yes                                    |
| `repartition()`  | Redistribute partitions          | Yes                                            |
| `coalesce()`     | Reduce partitions                | Usually avoids full shuffle                    |
| `join()`         | Combine DataFrames               | Often                                          |
| `broadcast join` | Join using small table broadcast | Avoids shuffling the broadcast side            |
| `explode()`      | Array → multiple rows            | Not inherently                                 |
| `row_number()`   | Window ranking                   | Often requires repartition/sort for the window |

---

# Progress

**Completed: 20 / 20 questions ✅**

Topics completed:

`Filtering` • `Selection` • `Conditional Columns` • `NULL Handling` • `Aggregations` • `Sorting` • `Window Functions` • `Deduplication` • `Arrays` • `Dates` • `Partitioning` • `Delta Lake` • `Performance Analysis`

---

# Why This Project

The goal of this project is not just to memorize PySpark syntax.

These exercises focus on reusable patterns that appear in:

* Data Engineering technical interviews
* ETL/ELT pipelines
* Data cleaning and transformation
* Databricks workloads
* Large-scale Spark processing
* Performance optimization

The project demonstrates practical understanding of PySpark DataFrame operations, window functions, data-quality handling, partition management, Delta Lake, and Spark execution plans.

---

## Next Steps

Future extensions can include:

* Broadcast joins
* Left semi and left anti joins
* Data skew handling
* Incremental loading
* Delta `MERGE`
* Structured Streaming
* Checkpointing
* Watermarking
* Schema evolution
* Small-file optimization
* `OPTIMIZE`
* Z-ORDER / Liquid Clustering
* Spark UI troubleshooting
