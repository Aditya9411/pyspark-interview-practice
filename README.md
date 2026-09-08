# PySpark Interview Practice

A hands-on PySpark practice project covering common Data Engineer interview patterns using an employee dataset.

## Skills Covered

- DataFrame filtering and column selection
- `withColumn`, `when`, and `otherwise`
- NULL handling
- Aggregations with `groupBy` and `agg`
- Sorting
- Window functions and Top-N
- Deduplication using the latest timestamp
- Arrays with `split`, `explode`, and `array_contains`
- Date transformations
- `repartition` vs `coalesce`
- Delta Lake writes
- Spark physical plans and shuffle detection with `explain()`

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

The practice dataset contains employee ID, name, department, salary, age, city, joining date, manager ID, skills, status, and update timestamp. It intentionally includes NULL salaries and duplicate employee IDs with different update timestamps for interview-style exercises.

## Databricks Setup

Upload `data/employees_pyspark_practice.csv` to a Databricks Volume and update `csv_path` in the notebook with your Volume path.

Example:

```python
csv_path = "/Volumes/<catalog>/<schema>/<volume>/employees_pyspark_practice.csv"
```

## Practice Questions

1. Filter employees with salary > 60,000.
2. Select name, department, and salary.
3. Find Data Engineering employees with salary > 70,000.
4. Create a salary category.
5. Find NULL salaries.
6. Replace NULL salaries with 0.
7. Count employees by department.
8. Calculate average salary by department.
9. Find maximum salary by department.
10. Sort by salary descending.
11. Find the top 3 highest-paid employees per department.
12. Deduplicate employee IDs and retain the latest record.
13. Convert skills into an array.
14. Explode skills into separate rows.
15. Find employees with Spark as a skill.
16. Calculate years of service.
17. Repartition by department.
18. Compare with coalesce.
19. Write cleaned data as Delta.
20. Inspect a physical plan and identify shuffle/Exchange.

## Why This Project

The goal is not just to memorize PySpark syntax. The exercises cover reusable patterns that appear in real data pipelines and Data Engineer technical interviews.
