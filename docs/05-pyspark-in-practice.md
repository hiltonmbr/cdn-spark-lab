# 🐍 PySpark in Practice

## What Is PySpark?

PySpark is Apache Spark's official Python API. It uses **Py4J** to bridge the Python interpreter and the **JVM** where Spark actually runs — when you call `df.filter(...)` in Python, Py4J translates the call to the JVM, which executes it using Catalyst + Tungsten. Your Python code does **not** run distributed across Executors for DataFrame operations — only the JVM logic runs distributed. Python in the Driver acts as a "remote control" for the Spark engine.

## PySpark vs. Pandas: When to Use Each

| Characteristic | Pandas | PySpark |
|---|---|---|
| Data volume | Fits in one machine's RAM | Distributed across hundreds of machines |
| Practical limit | ~10 GB (depends on RAM) | Petabytes |
| Execution model | Eager | **Lazy** (optimizes before running) |
| Overhead | None (local) | Significant (network, serialization, JVM) |
| Use cases | Exploratory analysis, small datasets | Data Lake ETL, production pipelines |

**Rule of thumb**: if your data comfortably fits in your laptop's RAM (up to ~10 GB), use Pandas/Polars/DuckDB — simpler and faster. If it doesn't fit on one machine, PySpark is the natural choice.

## SparkSession: The Entry Point

Since Spark 2.0, `SparkSession` unified the old entry points (`SparkContext`, `SQLContext`, `HiveContext`):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstJob") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/bronze/vendas/")
df.show(5)
```

## Spark Connect: A Thin Client for the Cluster

Traditionally, the Driver JVM had to run wherever you submitted the job from, with a direct network path to every Executor. **Spark Connect** (stable since Spark 3.5) decouples this: your Python process becomes a **thin gRPC client** that sends unresolved logical plans to a remote Spark Connect server, which runs the actual Driver and talks to the cluster. Your laptop never needs a JVM, a Hadoop classpath, or direct network reachability to Executors — just a gRPC connection.

```python
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

This is exactly what Cases B and D in this lab use: `make up-cluster` starts a `spark-connect` server inside Docker, and your notebook on the host connects to it as a thin client.

## DataFrames vs. RDDs, Side by Side

```python
# Modern way: DataFrames (recommended)
df = spark.read.csv("vendas.csv", header=True, inferSchema=True)
df.filter(df.valor > 100).groupBy("regiao").sum("valor").show()

# Legacy way: RDDs (avoid in new code)
rdd = spark.sparkContext.textFile("vendas.csv")
rdd.filter(lambda line: "ERROR" in line).count()
```

**Current recommendation**: always prefer the DataFrame API. It benefits from Catalyst and Tungsten — optimizations raw RDDs never get.

## The Python Performance Cliff: UDFs

Native functions (`pyspark.sql.functions.upper()`, `when()`, etc.) run entirely in the JVM — Catalyst understands and optimizes them. A **Python UDF** breaks that:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def normalize(name):          # runs row by row, in Python!
    return name.strip().upper()
```

Each row is **serialized (pickle) → sent to a Python process → executed → deserialized back**. Catalyst can't see inside the function — no predicate/projection pushdown, no code generation for that step.

## Pandas UDFs: Vectorization via Apache Arrow

Since Spark 2.3, **Pandas UDFs** use **Apache Arrow** to exchange data between the JVM and Python in **columnar batches** (not row by row), leveraging NumPy/Pandas vectorization — typically **3-100x faster** than a plain Python UDF:

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def normalize(names: pd.Series) -> pd.Series:
    return names.str.strip().str.upper()
```

**Rule of thumb**: always prefer native `pyspark.sql.functions`. If you need custom logic, reach for a Pandas UDF before a plain Python UDF.

## Spark SQL: Same Engine, SQL Syntax

```python
df.createOrReplaceTempView("vendas")

result = spark.sql("""
    SELECT regiao, SUM(valor) AS total
    FROM vendas
    WHERE ano = 2026
    GROUP BY regiao
    ORDER BY total DESC
""")
```

The DataFrame API and Spark SQL produce the **identical** physical execution plan, optimized by the same Catalyst Optimizer — the choice between them is readability, not performance.

## What You'll See in This Lab

Lab 05 introduces Spark Connect hands-on: you'll start the cluster, connect a thin client from your host, and confirm (via the Spark UI at `localhost:4040`) that execution really is happening in the Docker containers, not on your laptop.
