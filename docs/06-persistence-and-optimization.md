# 💾 Persistence & Optimization

## Cache and Persist: Reusing Data

By default, Spark **recomputes** an RDD/DataFrame every time an action is called on it — remember, evaluation is lazy and nothing is stored automatically. When the same data is used **multiple times**, that wastes work.

```python
vendas = spark.read.parquet("data/bronze/vendas/").filter("valor > 0")
vendas.cache()  # mark for in-memory retention after the first use

total = vendas.count()                           # computes and CACHES
avg = vendas.agg({"valor": "avg"}).collect()      # reuses the cache!
```

Without `cache()`, the second action (`agg`) would recompute the **entire** read+filter pipeline from scratch — exactly like the first one did. With cache, it reuses the result already materialized in the Executors' RAM.

## Storage Levels

`persist()` (more general than `cache()`, which is just a shortcut for `MEMORY_ONLY`) lets you choose exactly where and how to store cached data:

| Storage Level | Where | When to use |
|---|---|---|
| `MEMORY_ONLY` | RAM (deserialized) | Default; fastest, but loses data if it doesn't fit in RAM |
| `MEMORY_AND_DISK` | RAM, spills to disk if needed | Safer; avoids full recomputation when RAM isn't enough |
| `MEMORY_ONLY_SER` | RAM (serialized/compact) | Saves RAM at the cost of extra CPU for (de)serialization |
| `DISK_ONLY` | Disk only | Datasets too big for RAM, but still avoids rereading the original source |

Call `unpersist()` when a cached dataset won't be reused, to free memory for other operations.

## Broadcast Variables: Avoiding Shuffle in Joins

One of the most powerful uses of shared variables is the **Broadcast Join**: when one table is small (fits in a single node's memory) and the other is huge, replicating the small one to every Executor is far cheaper than shuffling both.

- **Without broadcast (Shuffle Join)**: both tables — even the small one — are partitioned and shuffled across the network so matching keys land on the same Executor. Expensive, especially when the large table has billions of rows.
- **With broadcast**: the small table (e.g. a 50-row `categorias` table) is copied **whole** into every Executor's memory. The join then happens **locally**, with zero Shuffle of the large table.

```python
from pyspark.sql.functions import broadcast

categorias = spark.read.parquet("data/bronze/categorias/")  # small: 50 rows
vendas = spark.read.parquet("data/bronze/vendas/")            # huge: millions of rows

result = vendas.join(broadcast(categorias), "id_categoria")
```

Spark SQL actually tries to detect this automatically via `spark.sql.autoBroadcastJoinThreshold` (default: 10 MB) — explicit `broadcast()` is useful when you want to **force** this strategy even if the optimizer's automatic estimate is wrong.

## Accumulators

While broadcast variables send data **Driver → Executors** (read-only), **Accumulators** go the opposite way: Executors safely **aggregate** values back to the Driver without race conditions.

```python
error_count = spark.sparkContext.accumulator(0)

def process_line(line):
    if "ERROR" in line:
        error_count.add(1)
    return line.upper()

logs.map(process_line).count()  # action that triggers processing
print(f"Total errors: {error_count.value}")
```

**Caution**: accumulators only guarantee exact counts within **actions**. If used inside a transformation that gets reexecuted (e.g. due to a Task retry), the value can be counted more than once — use them for debugging metrics, not business-critical logic.

## Smart Repartitioning

Expensive operations (`groupBy`, `join`, `distinct`) require a Shuffle. Repartitioning by a known key **once**, upfront, can make it reusable across several subsequent operations:

```python
vendas_by_regiao = vendas.repartition(200, "regiao")

vendas_by_regiao.groupBy("regiao").sum("valor")   # no additional shuffle
vendas_by_regiao.groupBy("regiao").count()          # no additional shuffle
```

`repartition()` itself costs a Shuffle, but if the same partitioning is reused by **multiple** downstream operations, the total cost can beat paying for the Shuffle repeatedly. `coalesce()` reduces partition count **without** a full Shuffle — ideal after an aggressive `filter()` that left partitions too small.

## Adaptive Query Execution (AQE)

Since Spark 3.0 (default since 3.2, `spark.sql.adaptive.enabled=true`), Spark can **replan execution in real time** using statistics collected during the Shuffle, instead of relying only on pre-run estimates:

- **Post-Shuffle partition coalescing** — reduces output partition count when they turn out smaller than expected.
- **Join strategy switching** — converts a `SortMergeJoin` into a `BroadcastHashJoin` at runtime, if a table turns out small enough — even if the original estimate was wrong.
- **Skew Join Optimization** — detects disproportionately large partitions (data skew) and automatically splits them into smaller sub-partitions processed in parallel.

## What You'll See in This Lab

Lab 07 (Case B) runs the same aggregation with AQE toggled on and off (`spark.conf.set("spark.sql.adaptive.enabled", ...)`) against a deliberately skewed slice of `vendas`, so you can watch AQE's Skew Join Optimization kick in live in the Spark UI.
