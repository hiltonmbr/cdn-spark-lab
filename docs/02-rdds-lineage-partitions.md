# 🧩 RDDs, Lineage, and Partitions

## What Is an RDD?

The **RDD (Resilient Distributed Dataset)** is Spark's original, foundational data abstraction — an immutable, distributed collection of objects, partitioned across cluster nodes, processed in parallel.

Four key properties:

- **Resilient** — recovers from failures automatically through *lineage* (not replicas).
- **Distributed** — data is sliced into **partitions** spread across Executors.
- **Immutable** — once created, an RDD never changes; transformations produce **new** RDDs.
- **Typed and lazy** — operations only execute when an action is called.

## Why Immutability?

At first glance, never being able to mutate data sounds limiting. In practice, it's what makes safe distributed computation possible:

- **No race conditions** — multiple Executors can read the same RDD simultaneously with zero risk of one overwriting what another is reading.
- **Safely cacheable** — a cached RDD never "goes stale," because its content never changes.
- **Reliable lineage** — since each RDD is generated deterministically from the previous one, Spark can always **recompute** a lost RDD by reapplying the same transformations.

> Instead of asking "how do I undo a change?", Spark's model asks "how do I recreate this data from its recipe?" — that's the essence of lineage.

## Lineage: Fault Tolerance Without Replication

HDFS guarantees fault tolerance by **replicating** each block 3x on disk — a significant storage cost. Spark solves the same problem completely differently: by **remembering the recipe**.

Each RDD keeps a reference to the RDD(s) that produced it and the transformation applied (e.g., "this RDD is the result of applying `filter(age > 18)` to the previous RDD"). If a partition is lost (an Executor crashed), Spark doesn't need a backup copy — it simply re-executes the recorded lineage chain, recreating only the lost partition.

This is fundamentally cheaper than replicating data: no extra storage cost during normal operation, only the (occasional) cost of recomputation when something fails.

## Partitions: The Unit of Parallelism

Just as HDFS slices files into 128 MB **blocks**, Spark slices RDDs into **partitions** — the smallest unit of work a single Task can process, on a single core of a single Executor.

- By default, reading a file from HDFS creates **one partition per HDFS block** (inheriting data locality natively).
- The number of partitions caps the **maximum degree of parallelism**: 100 partitions allow at most 100 concurrent Tasks.
- Too few partitions → idle cores, wasted parallelism.
- Too many partitions → excessive Task scheduling overhead.
- Rule of thumb: aim for **2-4 partitions per available CPU core**.

Today, most Spark applications use the **DataFrame** API (covered in docs/04) instead of raw RDDs — but every DataFrame is, under the hood, converted into RDD operations. Understanding RDDs means understanding the engine behind everything else.

## Creating RDDs

```python
# From an existing Python collection in the Driver's memory
rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5], numSlices=4)

# From a distributed file (HDFS, S3, local)
rdd2 = spark.sparkContext.textFile("path/to/logs.txt")

# From transforming another RDD
rdd3 = rdd2.filter(lambda line: "ERROR" in line)

print(rdd1.getNumPartitions())  # 4
print(rdd3.count())              # triggers execution (an action)
```

## What You'll See in This Lab

Lab 04 (Case A, local) makes lineage tangible: you'll build a short RDD chain, inspect `.toDebugString()` to see the lineage graph Spark recorded, and compare it against the equivalent DataFrame plan from `.explain()` in the same notebook.
