# PySpark Interview Practice

A hands-on **PySpark interview preparation project** covering common Data Engineer coding patterns using a realistic employee dataset.

The project focuses on practical PySpark operations commonly tested in **Data Engineering interviews**, including transformations, aggregations, window functions, deduplication, partitioning, Delta Lake, and performance analysis.

## Skills Covered

* DataFrame filtering and column selection
* `withColumn`, `when`, and `otherwise`
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

## Repository Structure

```text
pyspark-interview-practice/
├── README.md
├── data/
│   └── employees_pyspark_practice.csv
└── notebooks/
    └── pyspark_interview_questions.py
```

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

## Databricks Setup

Upload:

```text
data/employees_pyspark_practice.csv
```

to a Databricks Volume.

Then update `csv_path` in the PySpark file with your Volume path.

Example:

```python
csv_path = "/Volumes/<catalog>/<schema>/<volume>/employees_pyspark_practice.csv"
```

## Practice Questions

### DataFrame Basics

* [x] **1.** Filter employees with salary > 60,000.
* [x] **2.** Select name, department, and salary.
* [x] **3.** Find Data Engineering employees with salary > 70,000.
* [x] **4.** Create a salary category.
* [x] **5.** Find NULL salaries.
* [x] **6.** Replace NULL salaries with 0.

### Aggregations & Sorting

* [x] **7.** Count employees by department.
* [x] **8.** Calculate average salary by department.
* [x] **9.** Find maximum salary by department.
* [x] **10.** Sort by salary descending.

### Window Functions & Deduplication

* [ ] **11.** Find the top 3 highest-paid employees per department.
* [ ] **12.** Deduplicate employee IDs and retain the latest record.

### Arrays

* [ ] **13.** Convert skills into an array.
* [ ] **14.** Explode skills into separate rows.
* [ ] **15.** Find employees with Spark as a skill.

### Date Transformations

* [ ] **16.** Calculate years of service.

### Partitioning

* [ ] **17.** Repartition by department.
* [ ] **18.** Compare with coalesce.

### Delta Lake

* [ ] **19.** Write cleaned data as Delta.

### Performance Analysis

* [ ] **20.** Inspect a physical plan and identify shuffle / Exchange.

## Progress

**Completed: 10 / 20 questions**

Current topics completed:

`Filtering` • `Selection` • `Conditional Columns` • `NULL Handling` • `Aggregations` • `Sorting`

Next topics:

`Window Functions` → `Deduplication` → `Arrays` → `Dates` → `Partitioning` → `Delta Lake` → `Performance`

## Solutions

Solutions are available in:

```text
notebooks/pyspark_interview_questions.py
```

Each solution includes the corresponding interview question followed by the PySpark implementation.

## Why This Project

The goal of this project is not just to memorize PySpark syntax.

These exercises focus on reusable patterns that appear in:

* Data Engineering technical interviews
* ETL/ELT pipelines
* Data cleaning and transformation
* Databricks workloads
* Large-scale Spark processing
* Performance optimization

The repository will be continuously updated as additional PySpark interview patterns are practiced and implemented.
