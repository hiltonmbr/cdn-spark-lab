# 🔀 Transformations, Actions, and the DAG

## Two Categories of Operations

Every operation on an RDD (or DataFrame) falls into one of two categories:

- **Transformations** — take an RDD, produce another RDD. **Lazy**: they don't execute immediately, they just get recorded in the lineage. Examples: `map()`, `filter()`, `flatMap()`, `groupByKey()`, `reduceByKey()`, `join()`, `union()`.
- **Actions** — trigger the **real execution** of the whole accumulated transformation chain, returning a result to the Driver or writing to storage. Examples: `count()`, `collect()`, `first()`, `take(n)`, `reduce()`, `saveAsTextFile()`.

## Why Lazy Evaluation?

Unlike classic MapReduce (which executes each phase immediately), Spark **accumulates** transformations without running them until an action is called. This unlocks three real performance wins:

- **Global pipeline view** — the Catalyst Optimizer (for DataFrames) sees the **entire** operation chain before executing anything, so it can reorder, merge, or eliminate unnecessary steps.
- **Avoids wasted work** — if you chain 5 transformations but only ask for `take(10)`, Spark processes just enough to deliver those 10 rows, not the whole dataset.
- **Fewer data passes** — `filter()` followed by `map()` can be **fused** into a single pass per record instead of two.

```python
logs = spark.sparkContext.textFile("access.log")

# Nothing executes yet — these are just Transformations
errors = logs.filter(lambda line: "ERROR" in line)
words = errors.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda w: (w, 1))

# ONLY NOW does Spark build the DAG and execute everything at once:
result = pairs.reduceByKey(lambda a, b: a + b).collect()
```

Until the `collect()` line, not a single byte of the log file has been read.

## The DAG: Directed Acyclic Graph

Every time an action is called, the Driver converts the accumulated transformation chain into a **DAG** — a graph where each node is an RDD and each edge is a transformation. The DAG Scheduler analyzes this graph and splits it into **Stages** — groups of transformations that can run without moving data between machines (no *shuffle*).

"Acyclic" is literal: the graph never loops. Each RDD depends only on "earlier" RDDs in the pipeline, so Spark always knows the correct execution order and can parallelize everything without mutual dependencies.

## Narrow vs. Wide Dependencies

- **Narrow** — each partition of the "child" RDD depends on **at most one** partition of the "parent" RDD (e.g. `map()`, `filter()`). Executes **within the same Executor**, no network transfer — extremely fast.
- **Wide** — a partition of the "child" RDD depends on **several** partitions of the "parent" RDD, spread across different Executors (e.g. `groupByKey()`, `reduceByKey()`, `join()`). Requires a **Shuffle** — data crosses the network between Executors.

## Jobs, Stages, Tasks

1. **Job** — created on every **action** call. One application can trigger dozens of Jobs.
2. **Stage** — each Job splits into Stages, delimited by **Shuffle** points (wide dependencies). Everything inside a Stage is narrow-dependency and chains together without network movement.
3. **Task** — each Stage splits into Tasks — one Task per partition. The smallest unit of work, sent to a single core on a single Executor.

## The Shuffle: Spark's Bottleneck

Just as Shuffle & Sort was MapReduce's Achilles' heel, the **Shuffle** remains Spark's most expensive operation — even with everything else in memory. A shuffle requires **every** Executor to write intermediate data to local disk (shuffle files), then **other** Executors read them over the network, redistributing records by key (an all-to-all pattern). It involves serialization/deserialization, disk I/O, and network traffic between every pair of Executors.

## The Spark UI

Every Spark application exposes a web UI (port **4040** by default) with full execution visibility: Jobs, Stages (including whether a Shuffle happened), Tasks (individual timing — key for spotting **data skew**), Storage (cached RDDs/DataFrames), and SQL (Catalyst's physical execution plan). Diagnosing Spark slowness almost always starts here.

## What You'll See in This Lab

Lab 06 (Case B) deliberately triggers a Shuffle-heavy join (`funcionarios` — no broadcast) side-by-side with a Shuffle-free broadcast join (`empresas`), and has you compare both in the Spark UI's Stages tab: count the Exchange nodes, compare Stage durations, and spot the difference a Shuffle makes.
